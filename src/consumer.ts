import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "test-app",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "test-group" });
const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "test", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if(message.value.toString() == '123'){
        console.log("Received: ", {
          partition,
          offset: message.offset,
          value: message.value.toString(),
        });
      }else{
        throw new Error("no data")
      }
    },
  });
};
run().catch(console.error);
