import boto3
import tempfile
import os
import cv2
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


BUCKET = os.environ.get('BUCKET')
S3PATH = os.environ.get('S3PATH')
DB = os.environ.get('DB')


def fin_check():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DB)
    try:
        response = table.query(IndexName='S3PATH-index',KeyConditionExpression=Key('S3PATH').eq(S3PATH))
    except ClientError as e:
        logging.error(e.response['Error']['Message'])
    else:
        return len(response['Items']) > 0

def time_put(sec):
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(DB)

    response = table.put_item(Item={'User':'SyanaiShikaku', 'S3PATH': S3PATH, 'time': sec})


    return response

if __name__ == "__main__":
    if fin_check():
        logging.critical('{} is already dynamoDB')
    else:
        tmpdir = tempfile.TemporaryDirectory()
        tmp = tmpdir.name + '/'
        tmp_file = tmp + 'tmp.mp4'
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(BUCKET)
        try:
            bucket.download_file(S3PATH, tmp_file)
            cap = cv2.VideoCapture(tmp_file)                  # 動画を読み込む
            video_frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) # フレーム数を取得する
            video_fps = cap.get(cv2.CAP_PROP_FPS)                 # フレームレートを取得する
            video_len_sec = video_frame_count / video_fps
            response = time_put(video_len_sec)
        except Exception as e:
            logging.error('error_{}'.format(S3PATH))
            logging.error(str(e))