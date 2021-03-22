import argparse as agp
import getpass
import os

from myTools import MSSQL_DBConnector as mssql
from myTools import DBConnector as dbc
import myTools.ContentObfuscation as ce


try:
    import pandas as pd
except:
    mi.installModule("pandas")
    import pandas as pd



def printSplashScreen():
    print("*************************************************************************************************")
    print("\t THIS SCRIPT ALLOWS TO EXTRACT SURVEY DATA FROM THE SAMPLE SEEN IN SQL CLASS")
    print("\t IT REPLICATES THE BEHAVIOUR OF A STORED PROCEDURE & TRIGGER IN A PROGRAMMATIC WAY")
    print("\t COMMAND LINE OPTIONS ARE:")
    print("\t\t -h or --help: print the help content on the console")
    print("*************************************************************************************************\n\n")



def processCLIArguments()-> dict:
    
    retParametersDictionary:dict = None
    
    dbpassword:str = ''
    obfuscator: ce.ContentObfuscation = ce.ContentObfuscation()

    try:
        argParsingResults:agp.ArgumentParser = agp.ArgumentParser(add_help=True)

        argParsingResults.add_argument("-n", "--DSN", dest="dsn", \
                                action='store', default= None, help="Sets the SQL Server DSN descriptor file - Take precedence over all access parameters", type=str)

        #TODO
        retParametersDictionary = {
                 "dsn" : "None",        
                 "dbserver" : "Shraddha-PC",
                 "dbname" :"Survey_Sample_A19",
                 "dbusername" : "sa",
                 "dbuserpassword" : "Db1234",
                 "trustedmode" : "Yes",
                 "viewname" : "None",
                 "persistencefilepath": "None",
                 "resultsfilepath" : "None"
                }


    except Exception as e:
        print("Command Line arguments processing error: " + str(e))

    return retParametersDictionary


def getSurveyStructure(connector: mssql.MSSQL_DBConnector) -> pd.DataFrame:
    
    surveyStructResults = None
    surveyQuery:str = 'SELECT * FROM Survey_Sample_A19.dbo.Question' 
    surveyStructResults:pd.DataFrame = connector.ExecuteQuery_withRS(surveyQuery)
     
    #TODO
    return surveyStructResults



def doesPersistenceFileExist(persistenceFilePath: str)-> bool:

    success = True

   #TODO

    return success



def isPersistenceFileDirectoryWritable(persistenceFilePath: str)-> bool:
    success = True
    #TODO
    return success


def compareDBSurveyStructureToPersistenceFile(surveyStructResults:pd.DataFrame, persistenceFilePath: str) -> bool:
    
    same_file = False
    
    #TODO

    return same_file




def getAllSurveyDataQuery(connector: dbc.DBConnector) -> str:

    #IN THIS FUNCTION YOU MUST STRICTLY CONVERT THE CODE OF getAllSurveyData written in T-SQL, available in Survey_Sample_A19 and seen in class
    # Below is the beginning of the conversion
    # The Python version must return the string containing the dynamic query (as we cannot use sp_executesql in Python!)

    strQueryTemplateForAnswerColumn: str = """ COALESCE(( SELECT a.Answer_Value FROM Answer as a WHERE a.UserId = u.UserId AND a.SurveyId = <SURVEY_ID> AND a.QuestionId = <QUESTION_ID>), -1) AS ANS_Q<QUESTION_ID> """ 
    strQueryTemplateForNullColumnn: str = 'NULL AS ANS_Q<QUESTION_ID> '
    strQueryTemplateOuterUnionQuery: str = """SELECT UserId, <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> FROM [User] as u WHERE EXISTS ( SELECT * FROM Answer as a WHERE u.UserId = a.UserId AND a.SurveyId = <SURVEY_ID>)"""

    strCurrentUnionQueryBlock: str = ''
    strFinalQuery: str = ''

    #MAIN LOOP, OVER ALL THE SURVEYS

    # FOR EACH SURVEY, IN currentSurveyId, WE NEED TO CONSTRUCT THE ANSWER COLUMN QUERIES
	#inner loop, over the questions of the survey

    # Cursors are replaced by a query retrived in a pandas df
    surveyQuery:str = 'SELECT SurveyId FROM Survey ORDER BY SurveyId' 
    surveyQueryDF:pd.DataFrame = connector.ExecuteQuery_withRS(surveyQuery)

    #CARRY ON THE CONVERSION
    #TODO
    #OutterForLoop over surveyId
    for i,data in surveyQueryDF.iterrows():
        print("Value of i : ",i)
        currentSurveyId = data['SurveyId']
        print(currentSurveyId)
       
        currentQuestionCursorStr:str = """SELECT * FROM ( SELECT SurveyId, QuestionId, 1 as InSurvey FROM SurveyStructure WHERE SurveyId = %s UNION SELECT %s as SurveyId,Q.QuestionId,0 as InSurvey FROM Question as Q WHERE NOT EXISTS(SELECT *FROM SurveyStructure as S WHERE S.SurveyId = %s AND S.QuestionId = Q.QuestionId )) as t ORDER BY QuestionId; """ % (currentSurveyId,currentSurveyId,currentSurveyId)

        currentQuestionCursorDF:pd.DataFrame = connector.ExecuteQuery_withRS(currentQuestionCursorStr)
        strColumnsQueryPart:str='';
        for j,currQData in currentQuestionCursorDF.iterrows():
            print(currQData)
            currentSurveyIdInQuestion = currQData['SurveyId']
            currentQuestionID = currQData['QuestionId']
            currentInSurvey = currQData['InSurvey']

            
            if currentInSurvey == 0 :
                strColumnsQueryPart= strColumnsQueryPart \
                + strQueryTemplateForNullColumnn.replace('<QUESTION_ID>',str(currentQuestionID))
               # """ SET {strColumnsQueryPart} = {strColumnsQueryPart} 
               #REPLACE({strQueryTemplateForNullColumnn}, '<QUESTION_ID>',
               #{currentQuestionID}) """
            else :
                strColumnsQueryPart= strColumnsQueryPart \
                + strQueryTemplateForAnswerColumn.replace('<QUESTION_ID>',str(currentQuestionID))

               # """ SET {strColumnsQueryPart} = {strColumnsQueryPart} 
               # REPLACE({strQueryTemplateForAnswerColumn}, '<QUESTION_ID>',
               # {currentQuestionID}) """
            
            #"""SET {strColumnsQueryPart} = {strColumnsQueryPart},"""
            if j != len(currentQuestionCursorDF.index)-1 :
                 strColumnsQueryPart = strColumnsQueryPart + ', '
        ###Inner For loop ends

        ##BACK IN THE OUTER LOOP OVER SURVEYS
       #"""SET {strCurrentUnionQueryBlock}  = 
		#	REPLACE({strQueryTemplateOuterUnionQuery},
		#			'<DYNAMIC_QUESTION_ANSWERS>',
		#			{strColumnsQueryPart});"""
        strCurrentUnionQueryBlock = strCurrentUnionQueryBlock \
        + strQueryTemplateOuterUnionQuery.replace('<DYNAMIC_QUESTION_ANSWERS>',
					str(strColumnsQueryPart))

        strCurrentUnionQueryBlock = strCurrentUnionQueryBlock.replace('<SURVEY_ID>',
                                                                     str(currentSurveyId)) 

        #"""SET {strCurrentUnionQueryBlock} = 
		#	REPLACE({strCurrentUnionQueryBlock}, 
		#				'<SURVEY_ID>', {currentSurveyId});"""
        strFinalQuery = strFinalQuery + strCurrentUnionQueryBlock
        if i != len(surveyQueryDF.index)-1 :
            strFinalQuery = strFinalQuery + ' UNION '
    return strFinalQuery




def refreshViewInDB(connector: dbc.DBConnector, baseViewQuery:str, viewName:str)->None:
    
    if(connector.IsConnected == True):
        #TODO
        pass

        


def surveyResultsToDF(connector: dbc.DBConnector, viewName:str)->pd.DataFrame:
    
    results:pd.DataFrame = None

    #TODO


def main():
    
    cliArguments:dict = None

    printSplashScreen()

    try:
        cliArguments = processCLIArguments()
    except Except as excp:
        print("Exiting")
        return

    if(cliArguments is not None):
        
        #if you are using the Visual Studio Solution, you can set the command line parameters within VS (it's done in this example)
        #For setting your own values in VS, please make sure to open the VS Project Properties (Menu "Project, bottom choice), tab "Debug", textbox "Script arguments"
        #If you are trying this script outside VS, you must provide command line parameters yourself, i.e. on Windows
        #python.exe Python_SQL_Project_Sample_Solution --DBServer <YOUR_MSSQL> -d <DBName> -t True
        #See the processCLIArguments() function for accepted parameters

        try:
            connector = mssql.MSSQL_DBConnector(DSN = cliArguments["dsn"], dbserver = cliArguments["dbserver"], \
                dbname = cliArguments["dbname"], dbusername = cliArguments["dbusername"], \
                dbpassword = cliArguments["dbuserpassword"], trustedmode = cliArguments["trustedmode"], \
                viewname = cliArguments["viewname"])


            connector.Open()
            
            surveyFinalQryDF:pd.DataFrame =getAllSurveyDataQuery(connector)
            print(surveyFinalQryDF)

           # surveyStructureDF:pd.DataFrame = getSurveyStructure(connector)
            #print(surveyStructureDF)

          #if(doesPersistenceFileExist(cliArguments["persistencefilepath"]) == False):

              #  if(isPersistenceFileDirectoryWritable(cliArguments["persistencefilepath"]) == True):
                    
                    
                    #pickle the dataframe in the path given by persistencefilepath
                    #TODO

               #     print("\nINFO - Content of SurveyResults table pickled in " + cliArguments["persistencefilepath"] + "\n")
                    
                    #refresh the view using the function written for this purpose
                    #TODO
                    
          #  else:
                #Compare the existing pickled SurveyStructure file with surveyStructureDF
                # What do you need to do if the dataframe and the pickled file are different?
                #TODO
          #      pass #pass only written here for not creating a syntax error, to be removed
            
            #get your survey results from the view in a dataframe and save it to a CSV file in the path given by resultsfilepath
            #TODO

            print("\nDONE - Results exported in " + cliArguments["resultsfilepath"] + "\n")

            connector.Close()

        except Exception as excp:
            print(excp)
    else:
        print("Inconsistency: CLI argument dictionary is None. Exiting")
        return



if __name__ == '__main__':
    main()