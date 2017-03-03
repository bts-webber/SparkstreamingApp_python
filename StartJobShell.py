import json,os
from optparse import OptionParser

def main():
    with open("AppConfig.json") as file:
        config=json.load(file)
    parser=OptionParser(usage="python -j jobname [options]")
    parser.add_option("-j", "--jobname", dest="jobname", help='job name',
                      default="", type="string")
    parser.add_option("-m", "--mode", dest="mode", help='yarn cluster mode,"cluster" or "client"',
                      default="cluster", type="string")
    parser.add_option("-d", "--driver", dest="dm", help="driver-memory",
                      default="1", type="string")
    parser.add_option("-N", "--num-executors", dest="en", help="num-executors",
                      default="1", type="string")
    parser.add_option("-C", "--executor-cores", dest="ec", help="executor-cores",
                      default="2", type="string")
    parser.add_option("-M", "--executor-memory", dest="em", help="executor-cores",
                      default="1", type="string")
    (options,args)=parser.parse_args()
    yarn_job_name=config["App"][options.jobname]["app_name"]
    start_cmd = "spark-submit " \
                "--master yarn-%s " \
                "--deploy-mode %s " \
                "--name %s " \
                "--driver-memory %sg " \
                "--num-executors %s " \
                "--executor-cores %s " \
                "--executor-memory %sg"%(options.mode,options.mode,yarn_job_name,
                                         options.dm,options.en,options.ec,options.em)
    zip_jobs=os.popen("zip -r Jobs.zip ./Jobs")
    zip_jobs.read()
    zip_utils=os.popen("zip -r Utils.zip ./Utils")
    zip_utils.read()
    #get jar lib file
    jarlib_list=os.listdir("./lib")
    jarlib_cmd=" --jars "
    for j in range(len(jarlib_list)):
        jarlib_list[j]="./lib/"+jarlib_list[j]
    jarlib_cmd+=",".join(jarlib_list)
    #get python lib file
    pylib_list=os.listdir("./pylib")
    pylib_cmd=" --py-files "
    for p in range(len(pylib_list)):
        pylib_list[p]="./pylib/"+pylib_list[p]
    pylib_cmd+=",".join(pylib_list)
    #get python file or files
    local_list=os.listdir("./")
    local_file=[]
    file_cmd=" --files "
    for l in local_list:
        if os.path.isfile(l):
            if l[-3:]==".py" or l[-4:]==".zip":
                pylib_cmd+=",./"+l
            else:
                local_file.append(l)
    file_cmd +=","+ ",".join(local_file)
    #get schema files
    schema_list=os.listdir("./schema")
    for s in range(len(schema_list)):
        schema_list[s]="./schema/"+schema_list[s]
    file_cmd+=",".join(schema_list)
    cmd=start_cmd+pylib_cmd+file_cmd+jarlib_cmd+" ./main.py %s"%options.jobname
    print "[+]CMD:",cmd
    start_job=os.popen(cmd)
    start_job.read()
if __name__=="__main__":
    main()