import json
import sys
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime

consumer_key = "Vfw0Iv409dt77JUYFAFDkf5VA"
consumer_secret = "SLrVaRb4hodZya9DWAC4nhCbvYsmkdIvyJVkiEeHLhEPLdiLl0"
access_token = "466776246-bAoGtpw06dcZRfrxlRNoSSBqusMP65KXxamUrX0h"
access_token_secret = "tzDlUGi9sxPoMZ9jwXVs0CKLeKpUAIyCQCCPWoYLIy8Yx"

data = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
out = open(f'diseny_plus{data}.txt','w')

class MyListener(StreamListener):

    def on_data(self, data):
        sys.stdout.flush()
        itemString = json.loads(data)
        return sys.stdout.write(str(itemString))
    
    def on_error(self, status):
        print(status)



if __name__ == "__main__":
    l = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=["DisneyPlus","Disney+","Pixar","Avengers","StarWars"])
