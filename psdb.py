import psycopg2
from locust import User, task, between


class Pgdb(User):
	def __init__(self):
		self.conn = psycopg2.connect(user = "postgres",
                                  password = "postgres",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "usersdb")

		self.cur = self.conn.cursor()
		try:
			f = open("log.txt", "r")
		except:
			self.make_table()


	def make_table(self):
		try:
			self.cur.execute("CREATE TABLE users (_id varchar PRIMARY KEY,followers integer,friends integer,screenName varchar,tweet_text varchar,hashtags TEXT);")
			print('Creating Table...')
			f = open("log.txt", "a")
			f.write("Table is created!")
			f.close()
		except :
			pass

	
	def add_user(self,_id,followers,friends,screenName,tweet_text,hashtags):
		try:
			query = "INSERT INTO users(_id,followers,friends,screenName,tweet_text,hashtags) VALUES (%s,%s,%s,%s,%s,%s);"
			params = (_id,followers,friends,screenName,tweet_text,hashtags)
			self.cur.execute(query,params)
		except:
			pass

	@task		
	def search_user_test(self):
		_id = '5c56d59f00030f1d0458b6d0'
		self.cur.execute("SELECT * FROM users WHERE _id = '{0}' ".format(_id))
		row = self.cur.fetchall()
		print(row)

	def search_user(self,_id):
		try:
			self.cur.execute("SELECT * FROM users WHERE _id = '{0}' ".format(_id))
			row = self.cur.fetchall()
		except:
			print('Not valid user id')
		print(row)


	def done(self):
		self.conn.commit()
		self.cur.close()
		self.conn.close()

if __name__ == "__main__":
	pg = Pgdb()
	pg.search_user('5c56d59f00030f1d0458b6d0')