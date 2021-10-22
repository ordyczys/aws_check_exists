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
    file_in_reader = csv.reader(file_in, delimiter='|')

    #set output CSV file
    file_log = open(filename+".log",'w')
    file_log_writer = csv.writer(file_log, delimiter='|')

    for row in file_in_reader:
        row_count+=1
        object_name=row[0]
        try:
            aws_client.head_object(Bucket=aws_bucket,Key=object_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                file_log_writer.writerow(['FAILED', object_name])
            else:
                print("Something else has gone wrong. ({})".format(object_name))
        else:
            file_log_writer.writerow(['SUCCESS', object_name])
        
        if row_count%1000 == 0:
            print(f"{row_count} for {filename} finished")

t0 = threading.Thread(target=check_objects_exist, args=('gcp_objects_0.csv',))
t1 = threading.Thread(target=check_objects_exist, args=('gcp_objects_1.csv',))
t2 = threading.Thread(target=check_objects_exist, args=('gcp_objects_2.csv',))
t3 = threading.Thread(target=check_objects_exist, args=('gcp_objects_3.csv',))
t4 = threading.Thread(target=check_objects_exist, args=('gcp_objects_4.csv',))
t5 = threading.Thread(target=check_objects_exist, args=('gcp_objects_5.csv',))
t6 = threading.Thread(target=check_objects_exist, args=('gcp_objects_6.csv',))
t7 = threading.Thread(target=check_objects_exist, args=('gcp_objects_7.csv',))
t8 = threading.Thread(target=check_objects_exist, args=('gcp_objects_8.csv',))
t9 = threading.Thread(target=check_objects_exist, args=('gcp_objects_9.csv',))
ta = threading.Thread(target=check_objects_exist, args=('gcp_objects_a.csv',))
tb = threading.Thread(target=check_objects_exist, args=('gcp_objects_b.csv',))
tc = threading.Thread(target=check_objects_exist, args=('gcp_objects_c.csv',))
td = threading.Thread(target=check_objects_exist, args=('gcp_objects_d.csv',))
te = threading.Thread(target=check_objects_exist, args=('gcp_objects_e.csv',))
tf = threading.Thread(target=check_objects_exist, args=('gcp_objects_f.csv',))
ts = threading.Thread(target=check_objects_exist, args=('gcp_objects_s.csv',))

t0.start()
t1.start()
t2.start()
t3.start()
t4.start()
t5.start()
t6.start()
t7.start()
t8.start()
t9.start()
ta.start()
tb.start()
tc.start()
td.start()
te.start()
tf.start()
ts.start()

t0.join()
t1.join()
t2.join()
t3.join()
t4.join()
t5.join()
t6.join()
t7.join()
t8.join()
t9.join()
ta.join()
tb.join()
tc.join()
td.join()
te.join()
tf.join()
ts.join()

finish = time.perf_counter()
print(f'Finished in {round(finish-start,2)} second(s)')