import os
import sys

DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DIR)

from redis_utils import event_redis_client

name = 'buzzbreak-experiment-platform-config'

# BuzzBreak 指标
# 国家
COUNTRY_CODE = event_redis_client.hget(name, 'country_code').decode().split(',')

# 新闻实验关注的位置
NEWS_PLACEMENT = event_redis_client.hget(name, 'news_placement').decode().split(',')

# 视频实验关注的位置
VIDEO_PLACEMENT = event_redis_client.hget(name, 'video_placement').decode().split(',')

# 实验关注的维度
INDICATOR_DIMENSION = event_redis_client.hget(name, 'memories_indicator_dimension').decode().split(',')

# 召回实验名称
RECALL_EXPERIMENT = event_redis_client.hget(name, 'recall_experiment_name').decode().split(',')

# Rank实验名称
RANKING_EXPERIMENT = event_redis_client.hget(name, 'ranking_experiment_name').decode().split(',')

# 新视频按类型统计关注的位置
VIDEO_KIND_PLACEMENT = ["videos_tab_popular", "home_tab_for_you"]

# Katkat 指标

KATKAT_VIDEO_CHANNEL = ["home_tab_funny", "home_tab_society", "home_tab_gaming", "home_tab_cute", "home_tab_food", "home_tab_home", "home_tab_for_you"]

KATKAT_VIDEO_PLACEMENT = ["immersive_videos_home", "immersive_vertical_videos_home"]

KATKAT_AD_PLACEMENT = ["video_feed", "immersive_video_feed", "immersive_vertical_video_feed"]

# mysql响应码
MYSQL_INSERT_FAIL = 10
MYSQL_INSERT_SUCCCESS = 20