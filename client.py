import boto3
import json
import io
from multiprocessing.pool import ThreadPool as Pool
import argparse
import numpy as np
import time
import urllib3

statusCounts = {
    200: list(),
    400: list(),
    401: list()
}

json_good = {"items": [
        {
            "sku": "0000541",
            "price": "$15.99",
            "currency": "USD",
            "description": "widget (x-large)"
        } ,
        {
            "sku": "0000781",
            "price": "$1.99",
            "currency": "USD",
            "description": "widget (small)"   
        }
    ],
    "expiry": round(time.time()*1000) + 60000
}

json_bad = {"items": [
        {
            "sku": "0000541",
            "price": "$15.99",
            "currency": "USD",
            "description": "widget (x-large)"
        } ,
        {
            "sku": "0000781",
            "price": "$1.99",
            "currency": "USD",
            "description": "widget (small)"   
        }
    ]
}

def do_post(http, url, data):
    r = http.request(
        'POST',
        url,
        body=json.dumps(data).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    return r

def successful_post(request):
    statusCounts[request.status].append(request.status)
    #print("{}: success".format(request.status))

def failed_post(e):
    print("failed: {}".format(e))

def generate_logs(server_url, epochs):

    dist_a = np.random.normal(loc=1000, scale=2, size=1000).round()
    dist_b = np.random.normal(loc=100, scale=25, size=1000).round()
    elements = ["/record/save", "/item/save"]
    http = urllib3.PoolManager(num_pools=5)

    with Pool(processes=20) as pool:
        epoch = 0
        while epoch < epochs:
            print("Epoch {} out of {}".format(epoch+1, epochs))
            x = 0
            while x < dist_a.size:
                dist_urls = np.random.choice(elements, int(dist_a[x]), p=[0.990, 0.010])
                for url in dist_urls:
                    pool.apply_async(do_post, (http,"{}/{}".format(server_url, url),json_good), {}, successful_post, failed_post)
                y = 0 
                while y < int(dist_b[x]):
                    pool.apply_async(do_post, (http,"{}/{}".format(server_url, "record/save"),json_bad), {}, successful_post,failed_post)
                    y += 1
                x+=1
            epoch +=1

def main():
    parser = argparse.ArgumentParser(
    prog = "client",
    description = "Makes REST calls to a mock service deployed to an ECS cluster to generate patterns of log file output")
        
    parser.add_argument("server_url")
    parser.add_argument(
        "-", "--epochs", 
        default=1, 
        type=int,
        help="The number of times to generate the distribution of HTTP status codesin the logs")
    args = parser.parse_args()
    if not args.server_url:
        parser.print_usage()
        exit(1)

    generate_logs(args.server_url, args.epochs)
    print("HTTP status code distribution:\n")
    print("200: {}\n".format(len(statusCounts[200])))
    print("400: {}\n".format(len(statusCounts[400])))
    print("401: {}\n".format(len(statusCounts[401])))
                
if __name__ == '__main__':
    main()

