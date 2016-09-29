# -*- coding: utf-8 -*-
#!/usr/bin/env python3
#-------------------------------------------------------------------------------
# Name:
# Purpose:       This .py file retrieves tweets using tweetId saved in a 'datasetTweetIDs.txt' file
#
# Required libs: twython
# Author:        konkonst
#
# Created:       28/12/2015
# Copyright:     (c) ITI (CERTH) 2015
# Licence:       <apache licence 2.0>
#-------------------------------------------------------------------------------
import twython, glob, os, time, sys, json

CONSUMER_KEY = "<consumer key>"
CONSUMER_SECRET = "<consumer secret>"
OAUTH_TOKEN = "<application key>"
OAUTH_TOKEN_SECRET = "<application secret"
CONSUMER_KEY = 't2kdisdnfGZrLjiFoX91MIkxZ'
CONSUMER_SECRET = '9htfw7jDMOEJvVpiPQLpFa3k698k3rSHSP8hm2yQg4dDnY6qSM'
OAUTH_TOKEN = '1389288768-Zqk2gWFzAKWN60EE79L1970C9h3d36OrqXIJQsP'
OAUTH_TOKEN_SECRET = 'xxYCkC10OHMR8W1r8qyPdLRogXOgkV6DSHJ2H2BX1XpAq'
twitter = twython.Twython(CONSUMER_KEY, CONSUMER_SECRET,OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

remaining = twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/lookup']['remaining']
coolOff = twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/lookup']['reset'] - time.time()
if remaining < 1:
    print('please wait for approximately %int minutes for twitter api to cool off' %coolOff/60)
    sys.exit()

allIdFiles = glob.glob('./*TweetIDs.txt')
datasetNames = ['. '.join([str(idx+1),x.split('\\')[-1].rstrip('TweetIDs.txt')]) for idx,x in enumerate(allIdFiles)]
choice = input('Please select which dataset to retrieve (provide corresponding number):\n'+'\n'.join(datasetNames)+'\n>> ')
datasetName = datasetNames[int(choice)-1].split('. ')[-1]
if not os.path.exists('./'+datasetName):
    os.makedirs('./'+datasetName)

idFile = allIdFiles[int(choice)-1]
allIDs = [x.strip() for x in open(idFile,'r').readlines()]
batchIds = [allIDs[x:x+100] for x in range(0, len(allIDs), 100)]
batchLength = len(batchIds)
print('There are %s batches containing 100 tweetIds\n' %batchLength)
tweetDict, retTweetIds, counter = {}, [], 0
for idx,Ids in enumerate(batchIds):
    if not idx % 100:
        print('Retrieving batch %s of %s at %s' %(idx+1,batchLength,time.strftime("%H:%M||%d/%m ")))
    if len(tweetDict) > 30000:
        print('creating new file...')
        with open('./'+datasetName+'/raw'+str(counter)+'.json','w') as f:
                f.write('\n'.join([json.dumps(x) for x in tweetDict.values()]))
        tweetDict = {}
        counter+=1
	
    stringedIds = ','.join(Ids)
##    print('Ids: "'+stringedIds+'", index number: '+str(idx))
    done=False
    while not done:     
        #Twitter is queried 
        try:
            myresponse = twitter.lookup_status(id = stringedIds, include_entities = 1)        
            for k in myresponse:
                tweetDict[k['id_str']] = k
                retTweetIds.append(k['id_str'])
            done = True
        except Exception as e:
            tosleep = round(max(twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/lookup']['reset'] - time.time(),0)+2,2)
            print(str(e)+', sleeping for %s mins' %(tosleep/60))
            time.sleep(tosleep)
            pass
