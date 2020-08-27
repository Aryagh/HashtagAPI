from flask import Flask, jsonify, request
from preprocess import *


app = Flask(__name__)
pre = Process()


@app.route('/', methods=['GET'])
def params_get():
    hashtag = request.args.get('hashtag')
    try:
	    repetitive_hashtags = pre.relevant_hashtags(hashtag)
	    influenc_users = pre.influence_users(hashtag)
	    return {"repetitive_hashtags":repetitive_hashtags, "influenc_users":influenc_users}
	except:
		return 'Request another hashtag!'

if __name__ == '__main__':
    app.run()

#127.0.0.1:5000/?hashtag=StopWar