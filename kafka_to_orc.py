import json 
import pyorc
import os 
from pyorc.enums import StructRepr
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


def flatten_message(message_value):
    """ Osquery messages in Kafka contain keys: 'decorators' and 'columns' which are nested json objects 
        This function returns a flat dict by processing in order: decorations, root keys, columns
    """
    #print("debug message: ",message_value) 
    flat_message = {}
    # decorations
    for key in message_value['decorations']:
        flat_message.update({key: message_value['decorations'][key]})
    # root keys
    for key in message_value:
        if key not in ['decorations', 'columns']: 
            flat_message.update({key: message_value[key]})
    # columns
    for key in message_value['columns']:
        flat_message.update({key: message_value['columns'][key]})
    return flat_message


class Schemas:
    """ A class to handle the creation and storage of ORC file schemas and Trino create table statements 
        Based on the Kafka messages.
        The full list of schemas will be saved between runs in file: schemas.json
    """
    def __init__(self):
        self.schemas = {} 
        # define known types for certain message fields (keys), all others will be string  
        self.known_types = { 
                "calendarTime": "string",
                "counter": "int",
                "unixTime": "bigint",
                "epoch": "int",
                "numerics": "boolean"
              }
        if os.path.isfile("schemas.json"): 
            # read the schemas saved from last run  
            with open("schemas.json","r") as file:
                self.schemas = json.load(file)

    def add(self, message_value):
        first_column = True
        flat_message = flatten_message(message_value)
        table_name = flat_message['name']
        # determine the type of this column
        for key in flat_message:
            datatype = "string" # default 
            if key in self.known_types:
                datatype = self.known_types[key] # override default if column has another known type
            if first_column:
                orc_schema_string = "struct<" + key + ":" + datatype 
                trino_create_table_string = "CREATE TABLE osquery." + table_name + " (" + key + " " + datatype
                first_column = False
            else:
                orc_schema_string = orc_schema_string + "," + key + ":" + datatype 
                trino_create_table_string = trino_create_table_string + ", " + key + " " + datatype  

        orc_schema_string = orc_schema_string + ">"
        trino_create_table_string = trino_create_table_string + ") WITH (FORMAT = 'ORC', external_location = 'hdfs://localhost:50070/orc_files/"+table_name  
        
        new_schema = { message_value["name"]: { "orc_schema": orc_schema_string , 
                                                "trino_create_table": trino_create_table_string } 
                     }
        print('Adding new schema')
        print(new_schema)
        self.schemas.update(new_schema)
        
    def exists(self, table_name):
        if table_name in self.schemas: 
            return True
        else:     
            return False

    def get_orc_schema(self, table_name):
        return self.schemas[table_name]['orc_schema']
  
    def save(self):
        """ Saves the schemas to the file: orc_schemas.json """
        with open('schemas.json', 'w') as fp:
            json.dump(self.schemas, fp)


class OrcFiles:
    """ A class to create each ORC file and write records to them
        Each type of message will have it's own ORC file. 
        An ORC file is akin to a database table while a message is akin to a table row. 
    """
    def __init__(self):
        self.file_index = 0
        self.orc_files = {} # a dict to store the file mnemonic along with it's writer index 
        # e.g. { 'processes' : 0,
        #        'os_version': 1 }
        self.orc_writers = [] # the writers for each ORC file are store in this list 
        
    def add_file(self, file, schema):
        """ Create a new writer for this ORC file type (never seen before on this run) 
            file:   the file mnemonic, e.g. 'processes' or 'os_version'
            schema: the ORC file schema in format: struct<...
        """
        file_dict = { file: self.file_index }
        self.orc_files.update(file_dict)
        self.file_index = self.file_index + 1 
        current_datetime = datetime.now()
        # Format the datetime as yyyy-mm-dd_hh-mm-ss
        formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

        # check if the subdirectory exists for this file type yet, if not create it
        if not os.path.isdir("./orc_files/"+file):
            os.mkdir("./orc_files/"+file) 

        file_path = "./orc_files/"+file+"/"+file+"_"+formatted_datetime+".orc"
        print("Creating ORC file: ", file_path)
        with open(file_path, "wb") as output:
            with pyorc.Writer(output, schema, struct_repr=StructRepr.DICT) as writer:
                self.orc_writers.append(writer)
        
    def add_record(self, file, flat_message):
        file_index = self.orc_files[file]
        try:
            self.orc_writers[file_index].write(flat_message)
        except: 
            print("Error adding to file: ",file)
            print("Message: ", flat_message)
        
    def exists(self, file):
        if file in self.orc_files: 
            return True
        else: 
            return False


def main(): 
    schemas = Schemas()
    files = OrcFiles()

    # skip certain tables due to type issue
    skip_tables = ['pack_osquery-monitoring_osquery_info']

    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'group1',
        'auto.offset.reset': 'earliest',  # Start from the beginning initially
        'enable.auto.commit': False,  # Disable auto-commit of offsets
    }

    # Create a Kafka consumer
    consumer = Consumer(kafka_config)

    # Subscribe to the Kafka topic
    consumer.subscribe(['osqueryTopic'])

    i = 1
    try:
        while True:
            message = consumer.poll(1.0)  # Poll for new messages

            if message is None:
                continue

            message_json = json.loads(message.value().decode('utf-8'))
            try:
                table_name = message_json['name']
            except: 
                print("Error! Message does not have 'name' key")
                print(message_json)
                break 

            if table_name in skip_tables:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    break  # No more records to consume, exit the loop
                else:
                    print('Error: {}'.format(message.error()))
                    break

            # check if we have the schema for this message, if not create it
            if not schemas.exists(table_name):
                schemas.add(message_json)
                # save the new schema set to file: schemas.json
                schemas.save()

            # check if we have the ORC file for this message, if not create it
            if not files.exists(table_name):
                files.add_file(table_name, schemas.get_orc_schema(table_name))

            # write this message to it's ORC file
            files.add_record(table_name, flatten_message(message_json))

            # print progress every 500 records or so and commit the offset
            if i % 20 == 0:
                print("record count: ", i)
                consumer.commit(message)
            i += 1

    finally:
        consumer.close()
        # close each ORC file
        for writer in files.orc_writers:
            writer.close


if __name__ == "__main__":
    main()

