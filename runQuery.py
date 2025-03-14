from requests.auth import HTTPBasicAuth
import requests
import json
import collections
import pandas as pd
from io import BytesIO as IO
import xlsxwriter
import sys
import xml.etree.ElementTree as ET
import cba_assay
import base64
import asyncio 
from aiohttp_retry import RetryClient
from datetime import datetime
import configparser  

g_jrLs = []

def flatten_json(y): 
    out = {} 
  
    def flatten(x, name =''): 
          
        # If the Nested key-value  
        # pair is of dict type 
        if type(x) is dict: 
              
            for a in x: 
                flatten(x[a], name + a + '.') 
                  
        # If the Nested key-value 
        # pair is of list type 
        elif type(x) is list: 
              
            i = 0
              
            for a in x:                 
                flatten(a, name + str(i) + '.') 
                i += 1
        else: 
            out[name[:-1]] = x 
  
    flatten(y) 
    return out 

"""
CLASS : QueryHandler

"""
class QueryHandler():
    
    def __init__(self, email, password,coreFilter=None):
        
        # Get from config file
        public_config = configparser.ConfigParser()
        # /projects/galaxy/tools/cba
        public_config.read("./config/setup.cfg")
        tenant = public_config["CORE LIMS"]["tenant"]
        
        self.queryBase = tenant
        
        if coreFilter is None:
            self.filter = 'CBA' # defaults to CBA but can be overwritten for other experiments and assays
        else:
            self.filter = coreFilter
               
        self.email = email
        self.password = password 

        self.baseExpansion = None

        self.responses = []
        self.total_count = 0
        self.page_counter = 0 
        self.chunk = 10
        self.max_pagesize = 0
        self.max_records = 0

        self.skip_query = ""
    
    def controller(self):
        pass

    def setUpQuery(self):
        pass

    def runQuery(self, queryString, result_format=None):
        # Note: with async changes, queryString requires $count=true
        my_auth = HTTPBasicAuth(self.email, self.password)
        query = queryString
        df = None
        try:
            result = requests.get(query, auth=my_auth,headers = {"Prefer": "odata.maxpagesize=12000"})      

            if result_format == 'xml':
                return result.content  # We're done. Return what we got
            else: # default format is dataframe
                content = json.loads(result.content)  # Turn result's content into JSON
                if 'value' in content == False:
                    return None  #  Bail. No interesting results, i.e. values
                elif len(content['value']) == 0:
                    return None
                else:
                    valueLs = content['value']
                    num_entities = len(valueLs)
                    allJson = pd.DataFrame([flatten_json(d) for d in valueLs])

                    self.max_records = content.get("@odata.count")

                    if self.max_records > num_entities: # If the original number of entities is less than max assume we need to query again

                        self.max_pagesize = self.total_count = len(valueLs) #  Is this correct? I guess so...
                        self.skip_query = content.get("@odata.nextLink").replace("skiptoken=1", "skiptoken={}")
                        self.page_counter = 0

                        # # executes all chunks
                        loop = None
                        while self.total_count < self.max_records:
                            if loop is None:  # First time
                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                            else:
                                loop = asyncio.get_event_loop()

                            future = asyncio.ensure_future(self.run()) #, loop=loop)
                            loop.run_until_complete(future)

                        for page in self.responses:
                            json_data = json.loads(page.decode('utf-8'))
                            if len(json_data['value']) > 0:
                                allJson = pd.concat([allJson,[flatten_json(d) for d in json_data['value']]],axis=0,ignore_index=True, sort=True)

                        self.responses = []
                    df = pd.DataFrame(allJson)
                    return df
        except Exception as e:
            print("\nException occurred:" + repr(e) + " QUERY: " + query)
            return None

            
    def runLineQuery(self, queryString, result_format=None):
        # Returns a list of JRs
        jrSet = {""}
        jrSet = self.runLineQueryUniquify(queryString, result_format) # Recursive
        return sorted(jrSet)  # a list of iunique JRs
        
    def runLineQueryUniquify(self, queryString, result_format=None):
        # Returns a set of JRs
        
        my_auth = HTTPBasicAuth(self.email, self.password)
        result = requests.get(queryString, auth=my_auth,headers = {"Prefer": "odata.maxpagesize=12000"})     

        if result_format == 'xml':   # Never, ever true
            return result.content
        else: # default format is dataframe
            jrSet = {""}
            content = json.loads(result.content)

            if len(content['value']) == 0:
                return jrSet   # i.e. empty set
            else:
                self.max_records = content.get("@odata.count")
                if self.max_records > len(content['value']):
                    self.max_pagesize = self.total_count = len(content['value']) 
                    self.skip_query = content.get("@odata.nextLink")
                    self.page_counter = 0  # Needed??
                    
                json_data_ls = content['value']
                for jaxStrain in json_data_ls:
                    try:
                        if jaxStrain != None and "MOUSESAMPLE_STRAIN" in jaxStrain:
                            jrSet.add(jaxStrain["MOUSESAMPLE_STRAIN"]["Barcode"])
                    except Exception:
                                continue  # i.e. ignore...

                if self.skip_query:
                    jrSet.update(self.runLineQueryUniquify(self.skip_query, result_format=None))
                
                return  jrSet
            

    async def fetch(self, url, session):
        async with session.get(url, retry_attempts=5, retry_for_statuses={401}) as response:            #raise_for_status=True
            return await response.read()

    async def run(self):

        chunk = self.chunk # don't change instance variable
        tasks = []

        b64Val = base64.b64encode(f"{self.email}:{self.password}".encode()).decode()
        headers={"Authorization": f"Basic {b64Val}"}

        # adjust for partial page (fewer than max page size) at end
        if self.total_count + self.max_pagesize * chunk > self.max_records:
            chunk_count = self.max_records - self.total_count
            chunk = int((self.max_records - self.total_count) / self.max_pagesize + 1)
        else:
            chunk_count = self.max_pagesize * chunk        

        async with RetryClient(headers=headers, raise_for_status=False) as session:
                
            for i in range(self.page_counter, self.page_counter + chunk):
                task = asyncio.ensure_future(self.fetch(self.skip_query.format(i+1), session))                
                tasks.append(task)
                
            self.responses.extend(await asyncio.gather(*tasks))
            self.page_counter+=chunk
            self.total_count += chunk_count

    def writeFile(self, dfList):
        excel_file = IO()
        xlwriter = pd.ExcelWriter(excel_file, engine='xlsxwriter')
        for df in dfList: 
            df[1].to_excel(xlwriter, str(df[0])[:30])
        xlwriter.save()
        xlwriter.close()
        excel_file.seek(0)  #reset to beginning
        return excel_file

    def get_metadata(self):
        return self.runQuery(self.queryBase + "$metadata", result_format='xml')

    def get_experiments(self): # putting this here for now so we can get with service account credentials

        raw_metadata = self.get_metadata()

        xml_root = ET.fromstring(raw_metadata)

        for entity in list(xml_root):
            for entity2 in list(entity):
                xml_root = entity2

        experiments = [
            entity.attrib['Name']
                for entity in list(xml_root.findall('{http://docs.oasis-open.org/odata/ns/edm}EntityType'))
                    if 'BaseType' in entity.attrib 
                        and entity.attrib['BaseType'] == "pfs.EXPERIMENT"
                        and int(entity.attrib['Name'].find(self.filter+'_',0,len(self.filter)+1)) == 0]  # Filter on first 4 characters

        return experiments
    
    
    

"""

CLASS : CBAAssayHandler

"""
class CBAAssayHandler(QueryHandler):
    
    def __init__(self, cbbList, requestList, templateList, fromDate, toDate, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, email, password,coreFilter=None):
        QueryHandler.__init__(self, email, password,coreFilter)

        # using template_instance as a literal that must be replaced with the actual experiment name before running
        # I took out "",EXPERIMENT_ROOM" after  "EXPERIMENT_TESTER" - may need to add it back for CBA
        self.baseExpansion = r"?$count=true&$expand=EXPERIMENT/pfs.template_instance($expand=EXPERIMENT_PROTOCOL,EXPERIMENT_TESTER)," \
                            r"ENTITY/pfs.MOUSE_SAMPLE_LOT" \
                            r"($expand=SAMPLE/pfs.MOUSE_SAMPLE($expand=MOUSESAMPLE_STRAIN,MOUSESAMPLE_MOUSE)," \
                            r"MOUSESAMPLELOT_{0}BATCH($expand=BATCH_{0}REQUEST))".format(self.filter)

        # ASSAY_DATA expand is added later because it depends on experiment name

        self.cbbInitFilter = r"$filter=(ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_{0}BATCH/Barcode eq ".format(self.filter)
        self.cbbFilter = r" or ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_{0}BATCH/Barcode eq ".format(self.filter)

        self.reqInitFilter = r"$filter=(ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_{0}BATCH/BATCH_{0}REQUEST/Barcode eq ".format(self.filter)
        self.requestFilter = r" or ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_{0}BATCH/BATCH_{0}REQUEST/Barcode eq ".format(self.filter)

        # these are additonal criteria that are added to the request, batch or experiment values
        self.fromdateInitFilter = r"EXPERIMENT/pfs.template_instance/JAX_EXPERIMENT_STARTDATE ge "
        self.todateInitFilter = r"EXPERIMENT/pfs.template_instance/JAX_EXPERIMENT_STARTDATE le "
		
        self.publInitFilter = r"EXPERIMENT/pfs.template_instance/PUBLISHED eq True"
        self.unpublInitFilter = r"EXPERIMENT/pfs.template_instance/PUBLISHED eq False"
		
        self.activeFilter = r"Active eq True and " \
                             r"EXPERIMENT/Active eq True and " \
                             r"ENTITY/pfs.MOUSE_SAMPLE_LOT/Active eq True and " \
                             r"ENTITY/pfs.MOUSE_SAMPLE_LOT/SAMPLE/pfs.MOUSE_SAMPLE/Active eq True"

        self.jaxstrainFilter = r"ENTITY/pfs.MOUSE_SAMPLE_LOT/SAMPLE/pfs.MOUSE_SAMPLE/MOUSESAMPLE_STRAIN/Barcode eq '{0}'"
        
        self.cbbList = cbbList
        self.requestList = requestList
        self.template = templateList   # Set if the user has specified experiments in the GUI. otherwise we build it from batches or requests
        self.fromdate = fromDate
        self.todate = toDate
        self.published = publishedBool
        self.unpublished = unpublishedBool
        self.inactive = inactiveBool
        self.summary = summaryBool
        self.jaxstrain = jaxstrain

    
    def getExperimentList(self):
        # Retun a list of the experiment names for this request or batch
        rawExpList = []
        if len(self.cbbList) <= 0 and len(self.requestList) <= 0 : # Neither batches no requests. Bail.
            return rawExpList
            
        # noting a bug here to address later - experiments might be different
        # for each item in the list but only getting results for first
        if len(self.cbbList) > 0:
            query = self.queryBase + r"{0}_BATCH('{1}')/REV_EXPERIMENT_BATCH_{0}_EXPERIMENT_TRAIT?$count=true".format(self.filter,self.cbbList[0])
        elif len(self.requestList) > 0 :
            query = self.queryBase + r"{0}_REQUEST('{1}')/REV_EXPERIMENT_REQUEST_{0}_EXPERIMENT_TRAIT?$count=true".format(self.filter,self.requestList[0])
        
        df = self.runQuery(query)
        if df is not None:
            if 'EntityTypeName' in df.keys(): # If false then no experiments! Ergo, no data
                result = df['EntityTypeName'].to_dict()
                rawExpList = {v: None for k, v in result.items()}.keys()
           
        return rawExpList

    # A template is a list of experiment names. 
    # It comes from either the UI, or a batch barcode or any experiment barcode passed in from the UI.
    # If the first barcode in the list has no experiments associated with it, we return no results. Is that what we want?
    """
    -------------------------------------------------
    | EXPS  | BATCHES   | REQUEST   | ACTION        
    -------------------------------------------------
    |  YES  |  YES      |  YES      |  self.setUpQuery(self.cbbList, ...)
    -------------------------------------------------
    |  YES  |           |  YES      |  self.setUpQuery(self.requestList,...)
    -------------------------------------------------
    |  YES  |  YES      |           |  self.setUpQuery(self.cbbList,...)
    -------------------------------------------------
    |  YES  |           |           |  self.setUpQuery([], "", "")
    -------------------------------------------------
    |       |  YES      |  YES      |  self.getExperimentList(); self.setUpQuery(self.cbbList, ...)
    -------------------------------------------------
    |       |  YES      |           |  self.getExperimentList(); self.setUpQuery(self.cbbList, ...)
    -------------------------------------------------
    |       |           |  YES      |  self.getExperimentList(); self.setUpQuery(self.requestList,...)
    -------------------------------------------------
    |       |           |           |  self.setUpQuery([], "", "")
    -------------------------------------------------
    """

    def controller(self):
        # User specified BATCHES but no EXPERIMENTS
        if len(self.cbbList) > 0 and len(self.template) > 0:   #Covers case of multiple batches and multiple templates
            return self.setUpQuery(self.cbbList, self.cbbInitFilter, self.cbbFilter)
        # User specified REQUESTS and no EXPERIMENTS
        elif len(self.requestList) > 0 and len(self.template) > 0:
            return self.setUpQuery(self.requestList, self.reqInitFilter, self.requestFilter)
        # User specified only EXPERIMENTS
        elif len(self.cbbList) == 0 and len(self.requestList) == 0 and len(self.template) > 0:
            return self.setUpQuery([], "", "")
        # User specified no EXPERIMENTS but BATCHES *and* REQUESTS but not both
        elif len(self.template) == 0 and ((len(self.cbbList) > 0) or (len(self.requestList) > 0)): #needs batch XOR request constraint (why? mmm)
            self.template = self.getExperimentList()  # BATCHES OR REQS
            if len(self.cbbList) > 0:
                return self.setUpQuery(self.cbbList, self.cbbInitFilter, self.cbbFilter)
            else:
                return self.setUpQuery(self.requestList, self.reqInitFilter, self.requestFilter)
        else:
            # !! Due to XOR above this else clause is taken when we have BATCHES and REQUESTS but no EXPERIMENTS. Is that what we want?
            # edge case but handles no filters
            # returning empty file when no filters for now
            return self.setUpQuery([], "", "")
            
            
#  self.template must have experiments in it at this point
    def setUpQuery(self, entityList, initFilter, filter):
        dfList = []
        for template in self.template:

            baseExpansion = self.baseExpansion.replace("template_instance", template) 

            # ASSAY_DATA expand is added here because it depends on experiment name
            queryString = self.queryBase + template + '_SAMPLE' \
                + baseExpansion + f",ASSAY_DATA/pfs.{template.replace('_EXPERIMENT', '_ASSAY_DATA')}" \
                            f"($expand=EXPERIMENT_SAMPLE($expand=DERIVED_FROM" \
                            f"($expand=INTERMEDIATE_ASSAY_DATA/pfs.INTERMEDIATE_{template.replace('_EXPERIMENT', '_ASSAY_DATA')};$orderby=Sequence)))&"

            # create filter condition for each request or batch entered
            if len(entityList) > 0:
                
                for entity in entityList:
                    if entityList.index(entity) == 0:
                        # assay is what you want for the query parts above
                        queryString += initFilter + f"'{entity}' "
                    else:
                        queryString += filter + f" '{entity}' " 

                queryString += ")"

            queryString += self.build_filters(queryString)
            
            # replace placeholder string for template name and run query
            result = self.runQuery(queryString.replace("template_instance", template))
            # this line is self documenting :-)
            if result is not None:
                dfList.append((template.split('_EXPERIMENT')[0], result))

        if self.summary:                
        
            # get summary list of columns
            columnList = cba_assay.meta_column_order()

            for i in range(len(dfList)): # list of tuples (experiment name, dataframe)

                # get non-meta data and qc columns to append to end of summary columns
                keep = cba_assay.assay_columns(dfList[i][0]) # lookup name is experiment name from the tuple

                # if not specified default to the assay columns not identified as metadata
                if keep is None:
                    keep = [k for k in dfList[i][1].keys() if (k.find("ASSAY_DATA.") == 0 and k.find("EXPERIMENT_SAMPLE") < 0 and k not in columnList)
                            or (k.find("ASSAY_DATA.EXPERIMENT_SAMPLE") == 0 and k.find("INTERMEDIATE_ASSAY_DATA") >= 0)]
                else:
                    keep = keep + [k for k in dfList[i][1].keys() if k.find("ASSAY_DATA.EXPERIMENT_SAMPLE") == 0 and k.find("INTERMEDIATE_ASSAY_DATA") >= 0]

                summaryCols = columnList + keep

                # filter out columns not part of summary or qc
                dfCols = [c for c in summaryCols if c in dfList[i][1].columns]
                df = dfList[i][1][dfCols].copy()

                # update to preferred names
                df.rename(columns=cba_assay.meta_column_names(), inplace=True)
                dfList[i] = (dfList[i][0], df)
                
        return self.clientsideFilters(dfList)

    def build_filters(self, query):
        # build query filter from form inputs
        filters = ""

        append = "$filter" in query

        # User can request published AND unpublished, published OR unpublished, or NEITHER
        if self.published == True and self.unpublished == False:
            if append:
                filters += r" and (" + self.publInitFilter  + ")"
            else:
                filters += r"$filter=(" + self.publInitFilter  + ")"
                append = True
        elif self.published == False and self.unpublished == True:
            if append:
                filters += r" and (" + self.unpublInitFilter  + ")"
            else:
                filters += r"$filter=(" + self.unpublInitFilter  + ")"
                append = True
        # Else they're either both true or both false. Then we don't care, i.e. no filtering

        if self.fromdate:
            filterStr = self.fromdateInitFilter   + f"{self.fromdate}"
            if append:
                filters += r" and (" + filterStr + ")" 
            else:
                filters += r"$filter=(" + filterStr + ")"
                append = True

        if self.todate:
            filterStr = self.todateInitFilter   + f"{self.todate}"
            if append:
                filters += r" and (" + filterStr + ")" 
            else:
                filters += r"$filter=(" + filterStr + ")"
                append = True

        if not self.inactive: # default unchecked is to exclude inactive
            if append:
                filters += r" and (" + self.activeFilter  + ")"
            else:
                filters += r"$filter=(" + self.activeFilter  + ")"
                append = True

        # Do I have the value of jaxstrain at this point?
        """
        if self.jaxstrain:
            jaxstrainfilterStr = self.jaxstrainFilter.format(self.jaxstrain)
            if append:
                filters += r" and (" + jaxstrainfilterStr + ")" 
            else:
                filters += r"$filter=(" + jaxstrainfilterStr + ")"
                append = True
        """
        return filters

    def writeFile(self, dfList):
        # overriding parent method
        excel_file = IO()

        xlwriter = pd.ExcelWriter(excel_file, engine='xlsxwriter')

        for df in dfList: 
            df[1].to_excel(xlwriter, str(df[0])[:30])

            workbook = xlwriter.book
            worksheet = xlwriter.sheets[str(df[0])[:30]]

            # set experiment sample numeric columns with PFS precision settings
            df_format = pd.DataFrame()

            # get numeric field attributes with decimal precision values
            queryString = self.queryBase + f"ENTITY_TYPE('{df[0]}_ASSAY')/TYPE_ATTRIBUTES?$count=true&" \
                    "$expand=DATA_TYPE/pfs.FLOATING_POINT&" \
                    "$filter=(DATA_TYPE/EntityTypeName eq 'FLOATING_POINT' and DATA_TYPE/pfs.FLOATING_POINT/FORMAT_STRING ne null)"

            df_format = pd.concat([df_format,self.runQuery(queryString)],axis=0,ignore_index=True, sort=True)

            # get numeric field attributes with user defined format
            queryString = self.queryBase + f"ENTITY_TYPE('{df[0]}_ASSAY')/TYPE_ATTRIBUTES?$count=true&" \
                    "$expand=DATA_TYPE/pfs.USER_EQUATION&" \
                    "$filter=(DATA_TYPE/EntityTypeName eq 'USER_EQUATION' and DATA_TYPE/pfs.USER_EQUATION/FORMAT_STRING ne null)"

            df_format = pd.concat([df_format,self.runQuery(queryString)],axis=0,ignore_index=True, sort=True)

            if df_format.size > 0: # ignore if there are no number formatting results
                # set format for each EscapedName in the results dataframe
                for name, value in zip(df_format['EscapedName'],df_format['DATA_TYPE.FORMAT_STRING']):
                    # get the index number for the column name
                    if 'ASSAY_DATA.' + name in df[1].columns:
                        index = int(df[1].columns.get_loc('ASSAY_DATA.' + name))

                        cell_format = workbook.add_format({'num_format': value})
                        worksheet.set_column(index, index, None, cell_format)

        #xlwriter.save()
        xlwriter.close()
        excel_file.seek(0)  #reset to beginning
        return excel_file
        
        
    # This function removes all the rowms where the strain does not atch the jaxstrain filter if present
    # resultDataFrameLs is actually a list of tuples. 
    # The first element is the name of the experiment and the second is the dataframe.
    def clientsideFilters(self, resultDataFrameLs):  
        try:
            # If the JAXSTRAIN filter is set remove the non-complying entities.
            if self.jaxstrain is None or len(self.jaxstrain) == 0 or self.jaxstrain == '':
                return resultDataFrameLs  # i.e. do nothing
            
            if resultDataFrameLs is None or len(resultDataFrameLs) == 0:
                return resultDataFrameLs # i.e. do nothing
                
            for i in range(0,len(resultDataFrameLs)):
                a,df = resultDataFrameLs[i]
                # df is a Dataframe. Remove all the rows from b where b.Strain <> self.jaxstrain
                df = df[df.Strain == self.jaxstrain]
                resultDataFrameLs[i] = (a,df)
                
            
        except Exception as e:
            print("\nException occurred:" + repr(e))
        finally:
            return resultDataFrameLs
               
    
"""

CLASS : CBABatchBarcodeRequestHandler

"""
class CBABatchBarcodeRequestHandler(QueryHandler):
    
    def __init__(self, cbbList, requestList, templateList, fromDate, toDate, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, email, password,coreFilter=None):
        QueryHandler.__init__(self, email, password,coreFilter)

        # Currenty getting all the batches for a particular experiment. We may narrow the list down later
        # templateList is a list of experiment names, e.g. CBA_BODY_WEIGHT_EXPERIMENT
        self.baseExpansion = "CBA_BATCH?$expand=REV_EXPERIMENT_BATCH_template_instance&$select=Barcode&$count=true"

       
        # NOTE - THE FOLLOWING FILTERS ARE NOT CURRENTLY USED IN THIS CLASS BUT LEFT HERE IN CASE THAT CHANGES
        self.cbbInitFilter = r"$filter=(ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_{0}BATCH/Barcode eq ".format(self.filter)
        self.cbbFilter = r" or ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_{0}BATCH/Barcode eq ".format(self.filter)

        self.reqInitFilter = r"$filter=(ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_{0}BATCH/BATCH_{0}REQUEST/Barcode eq ".format(self.filter)
        self.requestFilter = r" or ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_{0}BATCH/BATCH_{0}REQUEST/Barcode eq ".format(self.filter)

        # these are additonal criteria that are added to the request, batch or experiment values
        self.fromdateInitFilter = r"EXPERIMENT/pfs.template_instance/JAX_EXPERIMENT_STARTDATE ge "
        self.todateInitFilter = r"EXPERIMENT/pfs.template_instance/JAX_EXPERIMENT_STARTDATE le "
		
        self.publInitFilter = r"EXPERIMENT/pfs.template_instance/PUBLISHED eq True"
        self.unpublInitFilter = r"EXPERIMENT/pfs.template_instance/PUBLISHED eq False"
		
        self.activeFilter = r"Active eq True and " \
                             r"EXPERIMENT/Active eq True and " \
                             r"ENTITY/pfs.MOUSE_SAMPLE_LOT/Active eq True and " \
                             r"ENTITY/pfs.MOUSE_SAMPLE_LOT/SAMPLE/pfs.MOUSE_SAMPLE/Active eq True"

        self.jaxstrainFilter = r"ENTITY/pfs.MOUSE_SAMPLE_LOT/SAMPLE/pfs.MOUSE_SAMPLE/MOUSESAMPLE_STRAIN/Barcode eq '{0}'"
        
        self.cbbList = cbbList
        self.requestList = requestList
        self.template = templateList   # Only one used at this point
        self.fromdate = fromDate
        self.todate = toDate
        self.published = publishedBool
        self.unpublished = unpublishedBool
        self.inactive = inactiveBool
        self.summary = summaryBool
        self.jaxstrain = jaxstrain

    
    # This function does the work setting up the query and calling PFS
    def controller(self):
        # User specified EXPERIMENTS
        return self.setUpQuery([], "", "")
        
            
#  self.template must have experiments in it at this point
    def setUpQuery(self, entityList, initFilter, filter):
        dfList = []
        for template in self.template:  # self.template is a list of experiment names, e.g. CBA_BODY_WEIGHT_EXPERIMENT
            baseExpansion = self.baseExpansion.replace("template_instance", template) # For batches query this is all we need
            queryString = self.queryBase + baseExpansion 
            result = self.runQuery(queryString)
            if result is not None:
                dfList.append(result)

        
        return dfList

    # Currently we are not filtering for BATCHES. That may change.
    def build_filters(self, query):
        # build query filter from form inputs
        filters = ""

        append = "$filter" in query

        # User can request published AND unpublished, published OR unpublished, or NEITHER
        if self.published == True and self.unpublished == False:
            if append:
                filters += r" and (" + self.publInitFilter  + ")"
            else:
                filters += r"$filter=(" + self.publInitFilter  + ")"
                append = True
        elif self.published == False and self.unpublished == True:
            if append:
                filters += r" and (" + self.unpublInitFilter  + ")"
            else:
                filters += r"$filter=(" + self.unpublInitFilter  + ")"
                append = True
        # Else they're either both true or both false. Then we don't care, i.e. no filtering

        if self.fromdate:
            filterStr = self.fromdateInitFilter   + f"{self.fromdate}"
            if append:
                filters += r" and (" + filterStr + ")" 
            else:
                filters += r"$filter=(" + filterStr + ")"
                append = True

        if self.todate:
            filterStr = self.todateInitFilter   + f"{self.todate}"
            if append:
                filters += r" and (" + filterStr + ")" 
            else:
                filters += r"$filter=(" + filterStr + ")"
                append = True

        if not self.inactive: # default unchecked is to exclude inactive
            if append:
                filters += r" and (" + self.activeFilter  + ")"
            else:
                filters += r"$filter=(" + self.activeFilter  + ")"
                append = True

        return filters
