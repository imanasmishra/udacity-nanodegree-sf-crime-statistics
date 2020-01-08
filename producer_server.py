from kafka import KafkaProducer
import json
import time

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            json_data = json.load(f)
            # reversing the list to simulate the crime data in JSON file
            json_data = reversed(json_data)
            for json_dict in json_data:
                #print (json_dict)
                # initialize message
                key = json_dict['crime_id'].encode('utf-8')
                message = self.dict_to_binary(json_dict)
                self.send(self.topic, key=key, value=message)
                time.sleep(0.01)
                                
    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')
