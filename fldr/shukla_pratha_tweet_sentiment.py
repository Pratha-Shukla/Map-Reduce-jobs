import sys
import json
import string

def lines(fp):
    print str(len(fp.readlines()))
    return	

afinn = open("AFINN-111.txt")
scores = {} 
    
for line in afinn:
    term, score = line.split("\t") #to split tweets into words
    scores[term] = int(score)

def main():

    sentiment_file = open(sys.argv[1])
    tweet_file = open(sys.argv[2])
                            

    count=0
    tot_word_count=0 # wordCount

    for each_line in tweet_file:
	senti_score = 0
	count=count+1

        common_dictionary = json.loads(each_line)
	
	if "lang" in common_dictionary.keys() and common_dictionary["lang"]=='en':
		if "text" in common_dictionary.keys():

	    		common_dictionary["text"] = common_dictionary["text"].encode('utf-8').lower().translate(None,string.punctuation)
			tokens = common_dictionary["text"].split()
			tot_word_count=tot_word_count+len(tokens)
			
			for token in tokens:
				if token in scores:
					senti_score=senti_score+scores[token]
			print senti_score

 #  lines(sentiment_file)

if __name__ == '__main__':
    main()
