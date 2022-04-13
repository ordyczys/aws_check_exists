import boto3
import botocore
import csv 
import time
import threading

start = time.perf_counter()

aws_bucket = 'vmnalias-repo-2020-e7158178-prod'
aws_client = boto3.client('s3')

def check_objects_exist(filename):
    print(f"Starting thread for {filename}")
    row_count=0

    # Set input CSV file
    file_in = open(filename, 'r')
    file_in_reader = csv.reader(file_in, delimiter=',')

    #set output CSV file
    file_log = open(filename+".log",'w')
    file_log_writer = csv.writer(file_log, delimiter='|')

    for row in file_in_reader:
        row_count+=1
        object_name=row[1]
        try:
            aws_client.head_object(Bucket=aws_bucket,Key=object_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                file_log_writer.writerow(['FAILED', object_name])
            else:
                print("Something else has gone wrong. ({})".format(object_name))
        else:
            file_log_writer.writerow(['SUCCESS', object_name])
        
        if row_count%100 == 0:
            print(f"{row_count} for {filename} finished")

t0 = threading.Thread(target=check_objects_exist, args=('VMS_version_ID_only.csv',))
t0.start()

t0.join()

finish = time.perf_counter()
print(f'Finished in {round(finish-start,2)} second(s)')
