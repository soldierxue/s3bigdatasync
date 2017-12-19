import json
import os

metadataFilePath = 'metadata.json'

def getFileData():
    with open(metadataFilePath) as json_input:
        data = json.load(json_input)
    
    return data
    
def getConfigurationValue(valueName):
    return getFileData()[valueName]

def updateConfigurationValue(valueName, value):
    data = getFileData()
    data[valueName] = value
    
    os.remove(metadataFilePath)
    fp = file(metadataFilePath, 'w')
    json.dump(data, fp)
    fp.close()
    