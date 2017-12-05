import tweepy

consumer_key = 'aBwzAuBhDX0KShy4vaQtWlRAY'
consumer_secret = 'xNZUisl44guCKjJLC2IBnMIDsj9cbCQcrGiUrXNdtQHxbkkPmy'
access_token = '360211556-hsxBM1esTmrBjIDBTVSAr1LrAjF8tkKs5sD13lLk'
access_token_secret = '8YhFUDGUMHFFgtjoQRFe13DbR50ZhAibGFudnzfrwaDA7'


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
