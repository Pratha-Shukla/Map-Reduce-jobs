import sys
import json
import string
import unicodedata

def lines(fp):
    print str(len(fp.readlines()))
    return	

afinnfile = open("AFINN-111.txt")
output_file=open("shukla_pratha_output.txt","w")

scores = {} # initialize an empty dictionary
    
for line in afinnfile:
    term, score = line.split("\t")
    scores[term] = int(score)

words_dictionary = {}

def main():

    total_word_count=0
    tweet_file = open(sys.argv[1])

    for each_line in tweet_file:

        common_dictionary = json.loads(each_line)
	
	if "lang" in common_dictionary.keys() and common_dictionary["lang"]=='en':
		if "text" in common_dictionary.keys():

	    		common_dictionary["text"] = common_dictionary["text"].encode('utf-8').lower().translate(None,string.punctuation)
			tokens = common_dictionary["text"].split()
			#print tokens
			total_word_count=total_word_count+len(tokens)

			for each_token in tokens:
				if each_token in words_dictionary.keys() and words_dictionary[each_token]>0:
					words_dictionary[each_token]=words_dictionary[each_token]+1
				else:
					words_dictionary[each_token]=1

	
    for each_token in words_dictionary:
	word_frequency =  float(words_dictionary[each_token])/total_word_count
	words_dictionary[each_token] = word_frequency
	
    
    print
    final=""

    for key, val in words_dictionary.iteritems():
        final=key+ "  "+ str(val)+"\n "
	output_file.write(final) 
        
 #  lines(sentiment_file)
    output_file.close()
if __name__ == '__main__':
    main()
