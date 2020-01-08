from kafka import KafkaConsumer
import json
import time

class ConsumerServer(KafkaConsumer):

    def __init__(self, bootstrap_servers, group_id, auto_offset_reset, enable_auto_commit, **kwargs):
        super().__init__(**kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit

    #we're consuming data from message
    def consume_data(self, topic_name):
        self.subscribe([topic_name])
        print (self.beginning_offsets(self.assignment()))
        #while True:
        #    message = self.poll(1.0)
        #    if message is None:
        #        print("no message received by consumer")
        #    else:
        #        #print (f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}, Key: {message.key}, Value: {message.value}")
        #        for partition in message.values():
        #            for record in partition:
        #                print (record.value)
        #    time.sleep(0.1)
        for message in self:
        	print (message)
        	print (f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}, Key: {message.key}, Value: {message.value}")
                                
def consume_data():
    subscriber = ConsumerServer(
        bootstrap_servers=["localhost:9092"],
        group_id="sf-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    topic_name="org-sf-police-department-service-calls"
    if subscriber is not None:
    	subscriber.consume_data(topic_name)
    	subscriber.close()
    


if __name__ == "__main__":
    consume_data()
