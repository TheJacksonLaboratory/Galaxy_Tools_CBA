from requests.auth import HTTPBasicAuth
import requests
import json
import collections
import pandas as pd
from io import BytesIO as IO
import sys
import xml.etree.ElementTree as ET
import cba_assay
import base64
import asyncio 
from aiohttp_retry import RetryClient

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


class QueryHandler():
    
    def __init__(self, email, password):
        self.queryBase = r"https://jacksonlabs.platformforscience.com/PROD/odata/"
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
        # print(queryString)

        my_auth = HTTPBasicAuth(self.email, self.password)
        query = queryString
        result = requests.get(query, auth=my_auth)

        # print(result)
        if result_format == 'xml':
            return result.content
        else: # default format is dataframe

            content = json.loads(result.content)

            if len(content['value']) == 0:
                return None
            else:
                allJson = pd.DataFrame([flatten_json(d) for d in content['value']])

                self.max_records = content.get("@odata.count")

                # print(self.max_records, len(content['value']))

                if self.max_records > len(content['value']):

                    # print(datetime.now())

                    self.max_pagesize = self.total_count = len(content['value']) 
                    self.skip_query = content.get("@odata.nextLink").replace("skiptoken=1", "skiptoken={}")
                    self.page_counter = 0

                    # # executes all chunks
                    loop = None
                    while self.total_count < self.max_records:
                        if loop is None:
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                        else:
                            loop = asyncio.get_event_loop()

                        future = asyncio.ensure_future(self.run()) #, loop=loop)
                        loop.run_until_complete(future)

                    for page in self.responses:
                        # print(page)
                        json_data = json.loads(page.decode('utf-8'))
                        if len(json_data['value']) > 0:
                            allJson = allJson.append([flatten_json(d) for d in json_data['value']], ignore_index=True, sort=True)

                    self.responses = []

                df = pd.DataFrame(allJson)
                return df

    async def fetch(self, url, session):
        async with session.get(url, retry_attempts=5, retry_for_statuses={401}) as response:            #raise_for_status=True
            # print (url)
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

        # print(self.total_count, self.max_pagesize, chunk)

        async with RetryClient(headers=headers, raise_for_status=False) as session:
        # async with RetryClient(headers=headers, raise_for_status=True) as session:
        #async with ClientSession(headers=headers) as session:
                
            for i in range(self.page_counter, self.page_counter + chunk):
                # task = (self.fetch(self.skip_query.format(i+1), session))
                task = asyncio.ensure_future(self.fetch(self.skip_query.format(i+1), session))                
                tasks.append(task)
                # print (str(i))
                
            self.responses.extend(await asyncio.gather(*tasks))
            self.page_counter+=chunk
            self.total_count += chunk_count

    def writeFile(self, dfList):
        excel_file = IO()
        xlwriter = pd.ExcelWriter(excel_file, engine='xlsxwriter')
        for df in dfList: 
            #if not df[1].empty:
            # print(str(df[0][0]))
            df[1].to_excel(xlwriter, str(df[0])[:30])
        xlwriter.save()
        xlwriter.close()
        excel_file.seek(0)  #reset to beginning
        #with open("/Users/lymanw/projects/mvp/testSite/output.xlsx", "wb") as f:
        #    f.write(excel_file.getbuffer())
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
                        and int(entity.attrib['Name'].find('CBA_',0,4)) == 0]

        return experiments


class CBAAssayHandler(QueryHandler):
    
    def __init__(self, cbbList, requestList, templateList, fromDate, toDate, publishedBool, inactiveBool, summaryBool, email, password):
        QueryHandler.__init__(self, email, password)

        # using template_instance as a literal that must be replaced with the actual experiment name before running
        self.baseExpansion = "?$count=true&$expand=EXPERIMENT/pfs.template_instance($expand=EXPERIMENT_PROTOCOL,EXPERIMENT_TESTER)," \
                            "ENTITY/pfs.MOUSE_SAMPLE_LOT" \
                            "($expand=SAMPLE/pfs.MOUSE_SAMPLE($expand=MOUSESAMPLE_STRAIN,MOUSESAMPLE_MOUSE)," \
                            "MOUSESAMPLELOT_CBABATCH($expand=BATCH_CBAREQUEST))" 

        # ASSAY_DATA expand is added later because it depends on experiment name

        self.cbbInitFilter = r"$filter=(ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_CBABATCH/Barcode eq "
        self.cbbFilter = r" or ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_CBABATCH/Barcode eq "

        self.reqInitFilter =r"$filter=(ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_CBABATCH/BATCH_CBAREQUEST/Barcode eq "
        self.requestFilter = r" or ENTITY/pfs.MOUSE_SAMPLE_LOT/MOUSESAMPLELOT_CBABATCH/BATCH_CBAREQUEST/Barcode eq "

        # these are additonal criteria that are added to the request, batch or experiment values
        self.fromdateInitFilter = r"EXPERIMENT/pfs.template_instance/JAX_EXPERIMENT_STARTDATE ge "
        self.todateInitFilter = r"EXPERIMENT/pfs.template_instance/JAX_EXPERIMENT_STARTDATE le "
        self.publInitFilter = r"EXPERIMENT/pfs.template_instance/PUBLISHED eq True"
        self.activeFilter = r"Active eq True and " \
                             "EXPERIMENT/Active eq True and " \
                             "ENTITY/pfs.MOUSE_SAMPLE_LOT/Active eq True and " \
                             "ENTITY/pfs.MOUSE_SAMPLE_LOT/SAMPLE/pfs.MOUSE_SAMPLE/Active eq True"

        self.cbbList = cbbList
        self.requestList = requestList
        self.template = templateList
        self.fromdate = fromDate
        self.todate = toDate
        self.published = publishedBool
        self.inactive = inactiveBool
        self.summary = summaryBool

    def getExperimentList(self):   #prepping for just having template

        # noting a bug here to address later - experiments might be different
        # for each item in the list but only getting results for first

        if len(self.cbbList) > 0:
            query = self.queryBase + f"CBA_BATCH('{self.cbbList[0]}')/REV_EXPERIMENT_BATCH_CBA_EXPERIMENT_TRAIT?$count=true"
        elif len(self.requestList) > 0 :
            query = self.queryBase + f"CBA_REQUEST('{self.requestList[0]}')/REV_EXPERIMENT_REQUEST_CBA_EXPERIMENT_TRAIT?$count=true"
        else:
            return []      

        # print(query)       
        df = self.runQuery(query)

        # need to check for case where no experiments
        if df is not None:
            # get list of unique experiments for the request/batch
            result = df['EntityTypeName'].to_dict()
            rawExpList = {v: None for k, v in result.items()}.keys()
        else:
            rawExpList = []

        # print(rawExpList)
        
        return rawExpList

    def controller(self):
        # print("controller")
        if len(self.cbbList) > 0 and len(self.template) > 0:   #Covers case of multiple batches and multiple templates
            return self.setUpQuery(self.cbbList, self.cbbInitFilter, self.cbbFilter)
        elif len(self.requestList) > 0 and len(self.template) > 0:
            return self.setUpQuery(self.requestList, self.reqInitFilter, self.requestFilter)
        elif len(self.cbbList) == 0 and len(self.requestList) == 0 and len(self.template) > 0:
            return self.setUpQuery([], "", "")
        elif len(self.template) == 0 and ((len(self.cbbList) > 0) ^ (len(self.requestList) > 0)): #needs batch XOR request constraint
            self.template = self.getExperimentList()
            if len(self.cbbList) > 0:
                return self.setUpQuery(self.cbbList, self.cbbInitFilter, self.cbbFilter)
            else:
                return self.setUpQuery(self.requestList, self.reqInitFilter, self.requestFilter)
        else:
            # edge case but handles no filters
            # self.template = self.get_experiments()

            # returning empty file when no filters for now
            return self.setUpQuery([], "", "")
    
    def setUpQuery(self, entityList, initFilter, filter):
        dfList = []
        for template in self.template:

            baseExpansion = self.baseExpansion.replace("template_instance", template) 

            # ASSAY_DATA expand is added here because it depends on experiment name
            queryString = self.queryBase + template + '_SAMPLE' \
                + baseExpansion + f",ASSAY_DATA/pfs.{template.replace('_EXPERIMENT', '_ASSAY_DATA')}" \
                            f"($expand=EXPERIMENT_SAMPLE($expand=DERIVED_FROM" \
                            f"($expand=INTERMEDIATE_ASSAY_DATA/pfs.INTERMEDIATE_{template.replace('_EXPERIMENT', '_ASSAY_DATA')})))&"

            # create filter condition for each request or batch entered
            if len(entityList) > 0:
                
                for entity in entityList:
                    if entityList.index(entity) == 0:
                        # assay is what you want for the query parts above
                        queryString += initFilter + f"'{entity}' "
                    else:
                        queryString += filter + f" '{entity}' " 

                queryString += ")"

            # print(queryString)

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

        return dfList

    def build_filters(self, query):
        # build query filter from form inputs
        filters = ""

        append = "$filter" in query

        if self.published:
            if append:
                filters += r" and (" + self.publInitFilter  + ")"
            else:
                filters += r"$filter=(" + self.publInitFilter  + ")"
                append = True

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

            df_format = df_format.append(self.runQuery(queryString), ignore_index=True, sort=True)

            # get numeric field attributes with user defined format
            queryString = self.queryBase + f"ENTITY_TYPE('{df[0]}_ASSAY')/TYPE_ATTRIBUTES?$count=true&" \
                    "$expand=DATA_TYPE/pfs.USER_EQUATION&" \
                    "$filter=(DATA_TYPE/EntityTypeName eq 'USER_EQUATION' and DATA_TYPE/pfs.USER_EQUATION/FORMAT_STRING ne null)"

            # print(queryString)
            df_format = df_format.append(self.runQuery(queryString), ignore_index=True, sort=True)

            if df_format.size > 0: # ignore if there are no number formatting results
                # set format for each EscapedName in the results dataframe
                for name, value in zip(df_format['EscapedName'],df_format['DATA_TYPE.FORMAT_STRING']):
                    # get the index number for the column name
                    if 'ASSAY_DATA.' + name in df[1].columns:
                        index = int(df[1].columns.get_loc('ASSAY_DATA.' + name))

                        cell_format = workbook.add_format({'num_format': value})
                        worksheet.set_column(index, index, None, cell_format)

        xlwriter.save()
        xlwriter.close()
        excel_file.seek(0)  #reset to beginning
        return excel_file
