# CA for Scalable Cloud Computing
** MSc Cloud in computing **

This project utilizes mutiple AWS services like S3, EC2, and CloudWatch to support parallel processing parts to support MapReduce, Streaming, and Hybrid parallel processing.

## 1. EC2 instances
Two EC2 instances are used:
   - Producer: Get dataset from S3 bucket and streams data to Kinesis. Configured with AutoScaling using a custom launch template. Normallyy keeps on t3.nano to minimize credit usage, and is upgraded to t3.large when executing performanc-heavy tasks like streaming or benchmarking.
   - Consumer: This EC2 is an instance used when executing streaming processing.By executing this instance, data coming from the Producer is received and passed to Streamlit to show the Top 10 words and sentiment analysis in real time. It is also used for task parallelism when implementing Hybrid Parallelism.
     
2. S3 Bucket
A bucket named "bookreview-results" is used to store:
   - Preproceed datasets
   - Benchmark outputs with visualization through matplotlib like graphs
   - Intermediate files (word count, sentiment)

3. Kinesis
   - A Kinesis named "book-reviews-stream" channel enables real-time communication between Producer.py and Consumer.py. The Producer pushes records in JSON format, and the Consumer subscribes and analyzes the data using a sliding time window.

4. CloudWatch & AutoScaling
   - Used to monitor EC2 metrics (CPU usage). Auto Scaling is triggered when usage exceeds 70%.
   - When the usage reaches over 70%, it makes new EC2 instance instead of the original EC2 instance and the new EC2 instance works in background.

5. Real-Time Dashboard (Streamlit)
   - The real-time dashboard is built using Streamlit and deployed on the Consumer EC2 instance.

### How to Access:
    1. SSH into the EC2 instance.  
    2. Run the following command:

        ```bash
        streamlit run Consumer_streamlit.py
   
    3. You can now view your Streamlit app in your browser.

        Local URL: http://localhost:8501  
        Network URL: http://172.31.xx.xx:8501  
        External URL: http://<ec2-public-ip>:8501

6. Requirements
   - pip install -r requirements.txt
   - requirements.txt
            boto3==1.34.21  
            pandas==2.2.1  
            matplotlib==3.8.4  
            numpy==1.26.4  
            multiprocess==0.70.15  
            streamlit==1.35.0

7. Contributors
   - Jiyoung Kim
   - MSc in Cloud Computing
   - National College of Ireland