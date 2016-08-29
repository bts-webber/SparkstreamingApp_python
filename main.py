from FromKafkaToKafkaJob import FromKafkaToKafkaJob as Job
import json
def main():
    with open("AppConfig.json") as configFile:
        config=json.loads(configFile.read())
    job=Job(config)
    job.startJob()
if __name__=="__main__":
    main()