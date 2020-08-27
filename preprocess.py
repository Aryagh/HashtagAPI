import numpy as np
import io
import json
import pandas as pd
from db import *
from psdb import *
from tqdm import tqdm

class Process:

    def __init__(self):
        self.fname = 'Tweets.txt'
        #Neo4j Object
        try:
            self.neo = NeoConnector()
        except:
            print('Can not Connect Neo4j sever')
        #Postgrace Object
        try:
            self.pg = Pgdb()
        except:
            print('Can not Connect Postgrace sever')
        self.hashtags_list, self.users_list, self.follower_list = self.read_data()
        self.M,self.H_num,self.num_H = self.Hashtags()
        self.sort()


    def clean_data(self,data):
            data = data.replace("ObjectId", "")
            data = data.replace("NumberLong", "")
            data = data.replace(")", "")
            data = data.replace("(", "")
            return data


    def Hashtags(self):
        #Flatten
        hashtags = set([val for sublist in self.hashtags_list for val in sublist])
        # Num of Hashtags
        N = len(set(hashtags))
        H_num = dict(zip(set(hashtags),np.arange(N)))
        num_H = dict(zip(np.arange(N),set(hashtags)))
        #Hashtags Adjacency Matrix
        M = np.zeros([N,N])
        for i,l in  enumerate(self.hashtags_list):
            for j in l:
                for k in l:
                    M[H_num[j],H_num[k]] += 1
        return [M,H_num,num_H]


    def relevant_hashtags(self,test_h):
        relev_h = []
        for i in np.argsort(self.M[self.H_num[test_h],:])[::-1][:10]:
            relev_h.append(self.num_H[i])
        return relev_h


    def read_data(self):
        users_list = []
        follower_list = []
        hashtags_list = []
        with io.open(self.fname, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = self.clean_data(line)
                tweet = json.loads(line)
                _id = tweet['_id']
                follower = tweet['TwitterUserEntityModel']['FollowersCount']
                hashtags = []
                users_list.append(_id)
                follower_list.append(follower)

                for hashtag in tweet['Entities']['Hashtag']:
                    hashtags.append(hashtag['Text'])
                hashtags_list.append(hashtags)

                #Add new user to postgrace
                self.pg.add_user(_id = _id,
                    followers=follower,
                    friends = tweet['TwitterUserEntityModel']['FriendsCount'],
                    screenName = tweet['TwitterUserEntityModel']['ScreenName'],
                    tweet_text = tweet['Text'],
                    hashtags=hashtags)

        #close postgrace
        self.pg.done()
        return [hashtags_list,users_list,follower_list]


    #sort users and hashtags by follower number
    def sort(self):
        self.sort_list = np.array(self.follower_list).argsort()
        self.users_list = np.array(self.users_list)[self.sort_list]
        self.hashtags_list = np.array(self.hashtags_list)[self.sort_list]
        return None


    def influence_users(self,test_h):
        influence_users_list = []
        for i,l in enumerate(self.hashtags_list):
            for j in l:
                if j == test_h:
                    influence_users_list.append(self.users_list[i])
                    break
            if len(influence_users_list) == 10:
                return influence_users_list
        return influence_users_list


    def neo_user_database(self):
        print('Creating user DataBase...')
        #Add users nodes
        for i,l in enumerate(self.hashtags_list[:100]):
            user_info = {'user_id':self.users_list[i],'Hashtags':l,'Followers':self.follower_list}
            self.neo.create_user_node(user_info=user_info)
        #Add users Connection
        for i1,l1 in enumerate(self.hashtags_list[:100]):
            for j1 in l1:
                for i2,l2 in enumerate(self.hashtags_list[:100]):
                    for j2 in l2:
                        if j1 == j2 and i1 != i2:
                            self.neo.create_user_relationship(self.users_list[i1],self.users_list[i2])


    def neo_hashtag_database(self):
        print('Creating hashtag DataBase...')
        #Add hashtags nodes
        for i in tqdm(list(self.num_H.values())[:500]):
            self.neo.create_hashtag_node(i)
        N = len(self.M)
        N = 500
        #Add hashtags Connection
        for i in tqdm(range(N)):
            for j in range(N):
                if self.M[i,j] != 0 and i != j:
                    try:
                        self.neo.create_hashtag_relationship(self.num_H[i],self.num_H[j])
                    except:
                        pass


if __name__ == "__main__":
    pre = Process()
    pre.neo_hashtag_database()
    pre.neo_user_database()
    # print('influence_users: ',pre.influence_users('Trump'))
    # print('relevant_hashtags: ',pre.relevant_hashtags('StopWar'))
