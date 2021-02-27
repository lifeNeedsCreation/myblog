# BuzzBreak 指标

# 时间秒数
ONE_MINUTE = 60
ONE_HOUR = 3600
ONE_DAY = 86400

COUNTRY_CODE = ["ID", "PH", "TH", "AR", "BR"]
# 新闻实验关注的位置
PLACEMENT = ["home_tab_for_you", "news_detail_activity", "home_tab_home"]
# 视频实验关注的位置
VIDEO_PLACEMENT = ["videos_tab_popular", "home_tab_for_you", "home_tab_for_you_video", "immersive_videos_tab_popular", "videos_tab_hiphop", "immersive_videos_tab_hiphop", "video_activity", "home_tab_home_video"]

# 新视频按类型统计关注的位置
VIDEO_KIND_PLACEMENT = ["videos_tab_popular", "home_tab_for_you"]


INDICATOR_DIMENSION = ["control", "treatment", "treatment_1", "treatment_2", "video_multiple_features_v1", "video_multiple_features_v2", "video_multiple_features", "news_actions", "news_multiple_features", "news_multiple_features_v1", "news_multiple_features_v2"]

# 召回实验名称
RECALL_EXPERIMENT = ["video_recall", "immersive_video_recall", "short_video_recall", "cold_start_video_recall"]

# Rank实验名称
RANK_EXPERIMENT = ["video_multiple_recommendation_model_v4"]



# Katkat 指标

KATKAT_VIDEO_CHANNEL = ["home_tab_funny", "home_tab_society", "home_tab_gaming", "home_tab_cute", "home_tab_food", "home_tab_home", "home_tab_for_you"]

KATKAT_VIDEO_PLACEMENT = ["immersive_videos_home", "immersive_vertical_videos_home"]

KATKAT_AD_PLACEMENT = ["video_feed", "immersive_video_feed", "immersive_vertical_video_feed"]

# mysql响应码
MYSQL_INSERT_FAIL = 10
MYSQL_INSERT_SUCCCESS = 20