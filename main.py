from __future__ import print_function
import json,sys
def main():
    if len(sys.argv)==2:
        exec("from Jobs import %s as Job"%sys.argv[1])
    else:
        sys.exit("main argv Error!")
    with open("AppConfig.json") as configFile:
        config=json.loads(configFile.read())
    job = Job(config)
    job.startJob()
if __name__=="__main__":
    main()