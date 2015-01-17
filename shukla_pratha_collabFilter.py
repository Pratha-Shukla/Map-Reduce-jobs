import sys
import json
import string
import math

ratings = {} # initialize an empty ratings dictionary

def main():
	ratings_file = open(sys.argv[1])
	user1=str(sys.argv[2])
	user2=str(sys.argv[3])
	item=str(sys.argv[4])
	k=int(sys.argv[5])
	ratings = readRatings(ratings_file)
	print "readRatings output", ratings
	sim=similarity(ratings[user1], ratings[user2])
	print "sim = ", sim
	nearest = nearestNeighbors(user1, ratings, k)
	print "nearestNeighbors: ", nearest
	prediction = predict(item, nearest, ratings)
	print "prediction for item", item, ": ", prediction
	 
def readRatings(ratings_file):
    
    # Write code to read ratings file and construct dictionary of dictionaries
	
	a=[]
	d={}
	for line in ratings_file:
		a=line.split("\t")
		key1=a[0]
		key2=a[2]
		value=a[1]
		if key1 not in d:
			d[key1]={}
			d[key1][key2]=value
		else:			
			d[key1][key2]=value
	return d
	
def similarity(user_ratings_1, user_ratings_2):

    # Write code to implement the Pearson correlation equation
    # Return the similarity of user 1 and user 2 based on their ratings
	User1_rating_sum=0.0
	User2_rating_sum=0.0
	User1_avg=0.0
	User2_avg=0.0
	User1_count=0.0
	User2_count=0.0
	cousers=0
	Numerator=0.0
	Denominator1=0.0
	Denominator2=0.0
	similarity=0.0
	for key, value in user_ratings_1.iteritems():
		User1_rating_sum=User1_rating_sum+float(value)
		User1_count=User1_count+1
	
	User1_avg=User1_rating_sum/User1_count
	
	for key, value in user_ratings_2.iteritems():
		User2_rating_sum=User2_rating_sum+float(value)
		User2_count=User2_count+1	
	User2_avg=User2_rating_sum/User2_count
	
	for key1, value1 in user_ratings_1.iteritems():
		for key2, value2 in user_ratings_2.iteritems():
			if key2==key1:
				Numerator=Numerator+(float(value1)-User1_avg)*(float(value2)-User2_avg)
				Denominator1=Denominator1+(float(value1)-User1_avg)*(float(value1)-User1_avg)
				Denominator2=Denominator2+(float(value2)-User2_avg)*(float(value2)-User2_avg)
	similarity=Numerator/((pow(float(Denominator1),0.5))*(pow(float(Denominator2),0.5)))
	return similarity


def nearestNeighbors(user_id, all_user_ratings, k):

    # Write code to determine the k nearest neighbors for user_id
	a=[]
	nearest=[]
	for key2 in all_user_ratings:
		if user_id != key2:
			x=similarity(all_user_ratings[user_id],all_user_ratings[key2])
			nearest.append((key2,x))
			
	nearest.sort(key=lambda similarity:similarity[1],reverse=True)
	return nearest[0:k]


def predict(item, k_nearest_neighbors, all_user_ratings):
    
    # Write code to predict the rating for the item given the k nearest neighbors of the user
	rated_movie={}
	prediction_num=0.0
	prediction_den=0.0
	prediction=0.0
	item=item+'\n'
	for key in all_user_ratings:
	    for key2 in all_user_ratings[key]:
			if key2==item:
				rated_movie[key]=all_user_ratings[key][key2]
	for x in k_nearest_neighbors:
		for key, value in rated_movie.iteritems():
			if x[0]==key:
				prediction_num=float(rated_movie[key])*float(x[1])+prediction_num
				prediction_den=float(x[1])+prediction_den				
	prediction=prediction_num/prediction_den
	return prediction
	

if __name__ == '__main__':
    main()
