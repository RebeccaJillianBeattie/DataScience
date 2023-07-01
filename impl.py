import pandas as pd
from pandas import read_csv, read_json, Series, DataFrame, merge
from json import load
from sqlite3 import connect
from rdflib import RDF, Graph, URIRef, Literal
import json
import sparql_dataframe
from sparql_dataframe import get
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
#OBJECT CLASSES

class IdentifiableEntity(object):
    def __init__(self, id={""}):        
        self.id = id
    
    def getIds(self):
        result = list()
        for iden in self.id:
            result.append(iden)
        result.sort()
        return result 


class Person(IdentifiableEntity):
    def __init__(self, givenName, familyName, id):
        self.givenName = givenName
        self.familyName = familyName

        super().__init__(id)
    
    def getGivenName(self):
        return self.givenName
    
    def getFamilyName(self):
        return self.familyName


class Publication(IdentifiableEntity):
    def __init__(self, id, title, author={""}, cites=[""], publicationYear=0, publicationVenue=""):
        self.publicationYear = publicationYear
        self.title = title
        self.author = author
        self.cites = cites
        self.publicationVenue = publicationVenue

        super().__init__(id)
    
    def getPublicationYear(self):
        if self.publicationYear != "":
            return self.publicationYear
        else:
            return None
    
    def getTitle(self):
        return self.title

    def getCitedPublications(self):
        if self.cites != [""]:
            return self.cites
        else:
            return None

    def getPublicationVenue(self):
        if self.publicationVenue != "":
            return self.publicationVenue
        else:
            return None
    
    def getAuthors(self):
        return self.author


class JournalArticle(Publication):
    def __init__(self, id, title, author, cites, publicationYear, publicationVenue, issue="", volume=""):
        self.issue = issue
        self.volume = volume

        super().__init__(id, title, author, cites, publicationYear, publicationVenue)

    def getIssue(self):
        if self.issue != "":
            return self.issue
        else:
            return None
    
    def getVolume(self):
        if self.volume != "":
            return self.volume
        else:
            return None

class BookChapter(Publication):
    def __init__(self, id, title, author, cites, publicationYear, publicationVenue, chapterNumber):
        self.chapterNumber = chapterNumber

        super().__init__(id, title, author, cites, publicationYear, publicationVenue)
    
    def getChapterNumber(self):
        return self.chapterNumber

class ProceedingsPaper(Publication):
    pass


class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name

        super().__init__(id)

    def getName(self):
        return self.name


class Venue(IdentifiableEntity):
    def __init__(self, id, title, publisher):
        self.title = title
        self.publisher = publisher 

        super().__init__(id)
    
    def getTitle(self):
        return self.title
    
    def getPublisher(self):
        return self.publisher

class Journal(Venue):
    pass

class Book(Venue):
    pass

class Proceedings(Venue):
    def __init__(self, id, title, publisher, event):
        self.event = event

        super().__init__(id, title, publisher)
    
    def getEvent(self):
        return self.event


####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
#RELATIONAL PROCESSORS

def CreateListFromDataFrameColumn(df, column): #Input df, string; output list
    list_from_df = []
    value = ""
    for idx, row in df.iterrows():
        value = row[column]
        list_from_df.append(value)
    return list_from_df


class QueryProcessor:
    pass


class RelationalProcessor(object):
    def __init__(self, dbPath=""):
        self.dbPath = dbPath

    def getDbPath(self):
        return self.dbPath

    def setDbPath(self, path):
        result = True
        if path != self.dbPath:
            self.dbPath, path = path, self.dbPath
        else:
            result = False
        return result         


class RelationalDataProcessor(RelationalProcessor):
    pass
        
    def uploadData(self, path):
        if ".json" in path:
            with open(path, "r", encoding="utf-8") as f:
                jsonr = load(f)
                
                #VenueId Table
                ven_list_of_lists = []
                id_list_gen = []
                doi_list = []
                for pub_doi in jsonr["venues_id"].keys():
                    id_list_local = []
                    for id in jsonr["venues_id"][pub_doi]:
                        doi_list.append(pub_doi)
                        id_list_gen.append(id)
                        id_list_local.append(id)
                    ven_list_of_lists.append(id_list_local)

                venue_internal_id = []
                for idx, val in enumerate(ven_list_of_lists):
                    for iden in val:
                        venue_internal_id.append("venue-" + str(idx))

                VenueId = DataFrame({"VenueInternalId": Series(venue_internal_id, dtype="string"), 
                                    "VenueId":Series(id_list_gen, dtype="string"), 
                                    "PublicationDoi":Series(doi_list, dtype="string")})
                
                #AuthorGroup Table
                pub_doi_global = []
                pub_doi = []
                given_name = []
                family_name = []
                aut_id = []
                for pub in jsonr["authors"].keys():
                    pub_doi_local = []
                    for author in jsonr["authors"][pub]:
                        pub_doi.append(pub)
                        pub_doi_local.append(pub)
                        given_name.append(author["given"])
                        family_name.append(author["family"])
                        aut_id.append(author["orcid"])
                    pub_doi_global.append(pub_doi_local)

                author_internal_ids = []
                for idx, val in enumerate(pub_doi_global):
                    for aut in val:
                        author_internal_ids.append("autgroup-" + str(idx))

                AuthorGroup = DataFrame({"AutGroupInternalId":Series(author_internal_ids, dtype="string"), 
                                        "givenName":Series(given_name, dtype="string"), 
                                        "AuthorId":Series(aut_id, dtype="string"), 
                                        "familyName":Series(family_name, dtype="string"), 
                                        "PublicationDoi":Series(pub_doi, dtype="string")})
            
                #CitesTable
                cites_list_global = []
                doi_list = [] 
                total_cites = []
                for pub_doi in jsonr["references"].keys():
                    if jsonr["references"][pub_doi] == []:
                        cites_list_global.append([])
                        doi_list.append(pub_doi)
                        total_cites.append([])
                    else:
                        cites_list_local = []
                        for cite in jsonr["references"][pub_doi]:
                            doi_list.append(pub_doi)
                            cites_list_local.append(cite)
                            total_cites.append(cite)
                        cites_list_global.append(cites_list_local)

                cites_internal_id = []
                for idx, ref in enumerate(cites_list_global):
                    if ref == []:
                        cites_internal_id.append("refs-" + str(idx))
                    else:
                        for pub in ref:
                            cites_internal_id.append("refs-" + str(idx))

                Cites = DataFrame({"CitesGroupInternalId": Series(cites_internal_id, dtype="string"), 
                                    "PublicationDoi": Series(doi_list, dtype="string"), 
                                    "DoiOfCitedPubs":Series(total_cites, dtype="string")})
                
                #Person Table
                family_names = []
                given_names = []
                id_vals = []
                for pub in jsonr["authors"].keys():
                    for person in jsonr["authors"][pub]:
                        family_names.append(person["family"])
                        given_names.append(person["given"])
                        id_vals.append(person["orcid"])

                person_internal_ids = []
                for idx, pers in enumerate(family_names):
                    person_internal_ids.append("pers-" + str(idx))

                Person = DataFrame({"PersonInternalId":Series(person_internal_ids, dtype="string"), 
                                    "givenName":Series(given_names, dtype="string"), 
                                    "familyName":Series(family_names, dtype="string"), 
                                    "PersonId":Series(id_vals, dtype="string")})
                Person = Person.drop_duplicates(subset=["PersonId"])
                
                #Organization Table
                pub_names = []
                pub_ids = []
                for key in jsonr["publishers"].keys():
                    pub_ids.append(key)
                    pub_names.append(jsonr["publishers"][key]["name"])

                pub_internal_ids = []
                for idx, code in enumerate(pub_ids):
                    pub_internal_ids.append("publisher-" + str(idx))

                Organization = DataFrame({"PublisherInternalId": Series(pub_internal_ids, dtype="string"), 
                                        "PublisherId": Series(pub_ids, dtype="string"), 
                                        "PublisherName": Series(pub_names, dtype="string")})
                
                #Upload these tables to database
                with connect(self.dbPath) as con:
                    VenueId.to_sql("VenueId", con, if_exists="replace", index=False)
                    AuthorGroup.to_sql("AuthorGroup", con, if_exists="replace", index=False)
                    Cites.to_sql("Cites", con, if_exists="replace", index=False)
                    Person.to_sql("Person", con, if_exists="replace", index=False)
                    Organization.to_sql("Organization", con, if_exists="replace", index=False)
                    con.commit
                return True

        elif ".csv" in path:
            publicationsr = read_csv(path,
                        keep_default_na=False, #avoid NaN for empty cells, return "" instead
                        dtype={
                            "id": str,
                            "title": str,
                            "type": str, 
                            "publication_year": str, #keep as str for simplicity; convert to int later
                            "issue": str,
                            "volume": str,
                            "chapter": str, #keep as str for simplicity; convert to int later
                            "publication_venue": str,
                            "venue_type": str,
                            "publisher": str,
                            "event": str #convert from float to string
                        })
            
            publicationsr = publicationsr.rename(columns={"publication_year":"publicationYear", 
                                                        "chapter":"chapterNumber", 
                                                        "publication_venue":"VenueTitle", 
                                                        "publisher":"Organization"})
            
            #Add internal id to all publications
            publication_internal_id = []
            for idx, row in publicationsr.iterrows():
                publication_internal_id.append("publication-" + str(idx))
            publicationsr.insert(0, "InternalId", Series(publication_internal_id, dtype="string"))
            
            #Venues: Journal Table
            Journal = publicationsr.query("venue_type == 'journal'")
            Journal = Journal[["id","VenueTitle", "Organization", "venue_type"]] 
            Journal = Journal.rename(columns={"venue_type":"venueType"})          
            
            #Venues: Book Table
            Book = publicationsr.query("venue_type == 'book'")
            Book = Book[["id","VenueTitle", "Organization", "venue_type"]]
            Book = Book.rename(columns={"venue_type":"venueType"}) 
            
            #Venues: Proceedings Table
            Proceedings = publicationsr.query("venue_type == 'proceedings'")
            Proceedings = Proceedings[["id","VenueTitle", "event", "Organization", "venue_type"]]
            Proceedings = Proceedings.rename(columns={"venue_type":"venueType"}) 
            
            #Publications: JournalArticle Table
            JournalArticle = publicationsr.query("type == 'journal-article'")
            JournalArticle = JournalArticle[["InternalId", "id", "publicationYear", "title", "issue", "volume", "type"]]
            
            #Publications: BookChapter Table
            BookChapter = publicationsr.query("type == 'book-chapter'")
            BookChapter = BookChapter[["InternalId", "id", "publicationYear", "title", "chapterNumber", "type"]]
            
            #Publications: ProceedingsPaper Table
            ProceedingsPaper = publicationsr.query("type == 'proceedings-paper'")
            ProceedingsPaper = ProceedingsPaper[["InternalId", "id", "publicationYear", "title", "type"]]
            
            #Upload these tables to database
            with connect(self.dbPath) as con:
                Journal.to_sql("Journal", con, if_exists="replace", index=False)
                Book.to_sql("Book", con, if_exists="replace", index=False)
                Proceedings.to_sql("Proceedings", con, if_exists="replace", index=False)
                JournalArticle.to_sql("JournalArticle", con, if_exists="replace", index=False)
                BookChapter.to_sql("BookChapter", con, if_exists="replace", index=False)
                ProceedingsPaper.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)
                con.commit
            return True
            
        else:
            return False


class RelationalQueryProcessor(RelationalProcessor, QueryProcessor):
    def __init__(self, dbPath=""):
        super().__init__(dbPath)
    
    def MultiValuePublicationInfoDataFrameFromDoi(self, doi): #Input: publication doi; output: df
        with connect(self.dbPath) as con:
            #Instantiate variables to avoid local assignment error
            aut_group = ""
            cites_group = ""
            venue_int_id = ""

            authorGroup_df = pd.read_sql("""
                                    SELECT AutGroupInternalId
                                    FROM AuthorGroup
                                    WHERE PublicationDoi='%s';
                                    """ % doi, con)
            for idx, row in authorGroup_df.iterrows():
                aut_group = row["AutGroupInternalId"]
                break
            
            cites_df = pd.read_sql("""
                                SELECT CitesGroupInternalId
                                FROM Cites
                                WHERE PublicationDoi='%s';
                                """ % doi, con)
            for idx, row in cites_df.iterrows():
                cites_group = row["CitesGroupInternalId"]
                break
            
            venueId_df = pd.read_sql("""
                                SELECT VenueInternalId
                                FROM VenueId
                                WHERE PublicationDoi='%s';
                                """ % doi, con)
            for idx, row in venueId_df.iterrows():
                venue_int_id = row["VenueInternalId"]
                break

            df_multi_values = DataFrame({"AutGroupInternalId": Series(aut_group, dtype="string"),
                                    "CitesGroupInternalId": Series(cites_group, dtype="string"),
                                    "VenueInternalId": Series(venue_int_id, dtype="string"),
                                    "PublicationDoi": Series(doi, dtype="string")})
       
            df_multi_values = df_multi_values.fillna(value="") #clean up NaN results
        
            return df_multi_values
       

    def createCompletePublicationInfoTableFromDoi(self, doi): #Input: publication doi, RelationalQueryProcessorPath; output: df
        with connect(self.dbPath) as con:
            df_ja = pd.read_sql("""
                                SELECT * 
                                FROM JournalArticle
                                WHERE id='%s';
                                """ % doi, con)
            df_bc = pd.read_sql("""
                                SELECT *
                                FROM BookChapter
                                WHERE id='%s';
                                """ % doi, con)
            df_pp = pd.read_sql("""
                                SELECT *
                                FROM ProceedingsPaper
                                WHERE id='%s';
                                """ % doi, con)
            
            multi_value_df = self.MultiValuePublicationInfoDataFrameFromDoi(doi)

            if df_ja.empty == False:
                df = merge(df_ja, multi_value_df, left_on="id", right_on="PublicationDoi")
                df = df[["InternalId", "id", "title", "AutGroupInternalId", "CitesGroupInternalId", "publicationYear", "VenueInternalId", "issue", "volume", "type"]]
            
            elif df_bc.empty == False:
                df = merge(df_bc, multi_value_df, left_on="id", right_on="PublicationDoi")
                df = df[["InternalId", "id", "title", "AutGroupInternalId", "CitesGroupInternalId", "publicationYear", "VenueInternalId", "chapterNumber", "type"]]

            else: #i.e. if df_pp.empty == False
                df = merge(df_pp, multi_value_df, left_on="id", right_on="PublicationDoi")
                df = df[["InternalId", "id", "title", "AutGroupInternalId", "CitesGroupInternalId", "publicationYear", "VenueInternalId", "type"]]

            df = df.fillna(value="") #clean up NaN results

            return df    

    def getJournalArticle(self, id, volume="", issue=""): #Input: Venue id, optional volume, optional issue; output: df
        #Find Venues with specified id
        with connect(self.dbPath) as con:
            df_venue = pd.read_sql("""
                                SELECT PublicationDoi
                                FROM VenueId
                                WHERE VenueId='%s';
                                """ % id, con)
            df_venue = df_venue.drop_duplicates() #It was found that some dois in the json had 2 identical venue id values (e.g. "doi:10.1177/19322968211005500")
            #Get publication id/doi list
            doi_list = CreateListFromDataFrameColumn(df_venue, "PublicationDoi")

            if issue != "" and volume != "":
                #Find JournalArticles ids/dois with specified issue and volume
                df_ja = pd.read_sql("""
                                SELECT id
                                FROM JournalArticle 
                                WHERE issue='%s' AND volume='%s';
                                """ % (issue, volume), con) 
                #Get publication id/doi list
                doi_list_issue_volume = CreateListFromDataFrameColumn(df_ja, "id")

                #Find the publications ids/dois in common with initial venue id list
                dois_in_common_iv = []
                for iden in doi_list_issue_volume:
                    if iden in doi_list:
                        dois_in_common_iv.append(iden)
                doi_list = dois_in_common_iv
        
            elif volume != "" and issue == "":
                #Identify JournalArticles with specified volume
                df_ja = pd.read_sql("""
                                SELECT id 
                                FROM JournalArticle
                                WHERE volume='%s';
                                """ % volume, con)
                #Get publication id/doi list
                doi_list_volume = CreateListFromDataFrameColumn(df_ja, "id")

                #Find the publications ids/dois in common with initial venue id list
                dois_in_common_v = []
                for iden in doi_list_volume:
                    if iden in doi_list:
                        dois_in_common_v.append(iden)
                doi_list = dois_in_common_v
        
            df = DataFrame()
            for doi in doi_list:                
                df_info = self.createCompletePublicationInfoTableFromDoi(doi)
                df = pd.concat([df, df_info])
            df = df.fillna(value="") #clean up NaN results

            return df     


    def createPublicationObjectFromDoiRelational(self, doi): #Input: publication doi, RelationalQueryProcessor; output: Publication object
        #Get basic info on the Publication
        #Instantiate Publication attributes to avoid local assignment errors
        pub_title = ""
        aut_group_id = ""
        cites_group_id = ""
        pub_year = ""
        pub_issue = ""
        pub_volume = ""
        pub_chapter = ""
        venue_int_id = ""
        pub_type = ""

        pub_info_df = self.createCompletePublicationInfoTableFromDoi(doi)
        for idx, row in pub_info_df.iterrows():
            pub_title = row["title"]
            aut_group_id = row["AutGroupInternalId"]
            cites_group_id = row["CitesGroupInternalId"]
            pub_year = row["publicationYear"]
            venue_int_id = row["VenueInternalId"]
            pub_type = row["type"]
            if pub_type == "journal":
                pub_issue = row["issue"]
                pub_volume = row["volume"]
            if pub_type == "book":
                pub_chapter = row["chapterNumber"]
    
        authors = self.getAuthorSetByAuthorGroupInternalId(aut_group_id)
        cited_doi_list = self.getIdsOfCitedPublications(cites_group_id)
        venue_id_list = self.getVenueIdListByVenueInternalId(venue_int_id)

        #Get info on the publication Venue + create Organization object for the publisher
        #Instantiate Venue attributes to avoid local assignment errors
        venue_title = ""
        venue_pub_id = ""
        venue_event = ""
        venue_type = ""
        venue_info_df = self.getVenueInfoByPublicationId(doi)
        for idx, row in venue_info_df.iterrows():
            venue_title = row["VenueTitle"]
            venue_pub_id = row["Organization"]
            venue_type = row["venueType"]
            if venue_type == "proceedings":
                venue_event = row["event"]
    
        publisher = self.getOrganizationObjectFromPublisherId(venue_pub_id)
    
        if venue_type == "journal":
            venue = Journal(venue_id_list, venue_title, publisher)
        elif venue_type == "book":
            venue = Book(venue_id_list, venue_title, publisher)
        else: #i.e. if venue_type == "proceedings"
            venue = Proceedings(venue_id_list, venue_title, publisher, venue_event)

        #Return the right sub-class of Publication object
        #Run the function recursively for publications cited by this publication
        if pub_type == "journal-article":
            if len(cited_doi_list) == 0: #base case
                return JournalArticle({doi}, pub_title, authors, [""], pub_year, venue, pub_issue, pub_volume)
            else: #recursive step 
                list_cited_pubs_objs = []
                for cited_pub in cited_doi_list:
                    cited_pub_obj = self.createPublicationObjectFromDoiRelational(cited_pub)
                    list_cited_pubs_objs.append(cited_pub_obj)
                return JournalArticle({doi}, pub_title, authors, list_cited_pubs_objs, pub_year, venue, pub_issue, pub_volume)
        
        if pub_type == "book-chapter":
            if len(cited_doi_list) == 0: #base case
                return BookChapter({doi}, pub_title, authors, [""], pub_year, venue, pub_chapter)
            else: #recursive step 
                list_cited_pubs_objs = []
                for cited_pub in cited_doi_list:
                    cited_pub_obj = self.createPublicationObjectFromDoiRelational(cited_pub)
                    list_cited_pubs_objs.append(cited_pub_obj)
                return BookChapter({doi}, pub_title, authors, list_cited_pubs_objs, pub_year, venue, pub_chapter)
        
        if pub_type == "proceedings-paper":
            if len(cited_doi_list) == 0: #base case
                return ProceedingsPaper({doi}, pub_title, authors, [""], pub_year, venue)
            else: #recursive step
                list_cited_pubs_objs = []
                for cited_pub in cited_doi_list:
                    cited_pub_obj = self.createPublicationObjectFromDoiRelational(cited_pub)
                    list_cited_pubs_objs.append(cited_pub_obj)
                return ProceedingsPaper({doi}, pub_title, authors, list_cited_pubs_objs, pub_year, venue)
        

    def getAuthorSetByAuthorGroupInternalId(self, internalId): #Supplementary method. Input: string; output: set of Person objects
        with connect(self.dbPath) as con:
            author_group_df = pd.read_sql("""
                                            SELECT *
                                            FROM AuthorGroup
                                            WHERE AutGroupInternalId='%s';
                                            """ % internalId, con)
            author_set = set()
            for idx, row in author_group_df.iterrows():
                pers_given_name = row["givenName"]
                pers_family_name = row["familyName"]
                pers_id = row["AuthorId"]
                person = Person(pers_given_name, pers_family_name, {pers_id})
                author_set.add(person)
            
            return author_set
    

    def getIdsOfCitedPublications(self, internalId): #Supplementary method. Input: string; output: list
        with connect(self.dbPath) as con:
            cites_df = pd.read_sql("""
                                    SELECT DoiOfCitedPubs
                                    FROM Cites
                                    WHERE CitesGroupInternalId='%s';
                                    """ % internalId, con)
            cited_pubs_doi_list = CreateListFromDataFrameColumn(cites_df, "DoiOfCitedPubs")

            return cited_pubs_doi_list
    

    def getVenueIdListByVenueInternalId(self, internalId): #Supplementary method. Input: string; output: list
        with connect(self.dbPath) as con:
            venue_ids_df = pd.read_sql("""
                                        SELECT VenueId
                                        FROM VenueId
                                        WHERE VenueInternalId='%s';
                                        """ % internalId, con)
            venue_id_list = CreateListFromDataFrameColumn(venue_ids_df, "VenueId")
        
            return venue_id_list
    

    def getVenueInfoByPublicationId(self, doi): #Supplementary method. Input: string; output: df
        with connect(self.dbPath) as con:
            venue_j = pd.read_sql("""
                                SELECT *
                                FROM Journal
                                WHERE id='%s';
                                """ % doi, con)
            venue_b = pd.read_sql("""
                                    SELECT *
                                    FROM Book
                                    WHERE id='%s';
                                    """ % doi, con)
            venue_p = pd.read_sql("""
                                    SELECT *
                                    FROM Proceedings
                                    WHERE id='%s';
                                    """ % doi, con)
            
            if venue_j.empty == True and venue_b.empty == True:
                venue_type = Proceedings
            elif venue_b.empty == True and venue_p.empty == True:
                venue_type = Journal
            else: #i.e. if venue_j.empty == True and venue_p.empty == True:
                venue_type = Book
            
            if venue_type == Journal:
                return venue_j
            elif venue_type == Book:
                return venue_b
            else: #i.e. if venue_type == Proceedings
                return venue_p
    
    def getVenueInfoByVenueId(self, id): #Supplementary method to help with GenericQueryProcessor. Input: string; output: df
        with connect(self.dbPath) as con:
            #Obtain an id/doi of a publication associated with this venue
            pub_doi = ""
            venue_ids_df = pd.read_sql("""
                                        SELECT PublicationDoi
                                        FROM VenueId
                                        WHERE VenueId='%s';
                                        """ % id, con)
            for idx, row in venue_ids_df.iterrows():
                pub_doi = row["PublicationDoi"]
                break

            #Use this doi to get more info on the Venue
            venue_j = pd.read_sql("""
                                SELECT *
                                FROM Journal
                                WHERE id='%s';
                                """ % pub_doi, con)
            venue_b = pd.read_sql("""
                                    SELECT *
                                    FROM Book
                                    WHERE id='%s';
                                    """ % pub_doi, con)
            venue_p = pd.read_sql("""
                                    SELECT *
                                    FROM Proceedings
                                    WHERE id='%s';
                                    """ % pub_doi, con)
            
            if venue_j.empty == True and venue_b.empty == True:
                venue_type = Proceedings
            elif venue_b.empty == True and venue_p.empty == True:
                venue_type = Journal
            else: #i.e. if venue_j.empty == True and venue_p.empty == True:
                venue_type = Book
            
            if venue_type == Journal:
                return venue_j[["VenueTitle", "Organization", "venueType"]]
            elif venue_type == Book:
                return venue_b[["VenueTitle", "Organization", "venueType"]]
            else: #i.e. if venue_type == Proceedings
                return venue_p[["VenueTitle", "Organization", "venueType"]]
    

    def getOrganizationObjectFromPublisherId(self, publisherId): #Supplementary method for createPublicationObjectFromDoiRelational function and to help with GenericQueryProcessor. Input: string; output: Organization object
        with connect(self.dbPath) as con:
            organ_df = pd.read_sql("""
                                SELECT *
                                FROM Organization
                                WHERE PublisherId='%s';
                                """ % publisherId, con)
            organ_name = ""
            for idx, row in organ_df.iterrows():
                organ_name = row["PublisherName"]
            
            return Organization({publisherId}, organ_name)
    
        
    def getPublicationsPublishedInYear(self, year):
        with connect(self.dbPath) as con:
            year = str(year)
            #Identify and join dois of all publications published in this year
            df_ja = pd.read_sql("""
                                SELECT id
                                FROM JournalArticle
                                WHERE publicationYear='%s';
                                """ % year, con)
            df_bc = pd.read_sql("""
                                SELECT id
                                FROM BookChapter
                                WHERE publicationYear='%s';
                                """ % year, con)
            df_pp = pd.read_sql("""
                                SELECT id
                                FROM ProceedingsPaper
                                WHERE publicationYear='%s';
                                """ % year, con)
            df = pd.concat([df_ja, df_bc, df_pp])

            #Use ids/dois of these publications in order to get complete publication info using function
            pub_doi_list = CreateListFromDataFrameColumn(df, "id")
            df = DataFrame()
            for pub_doi in pub_doi_list:
                df_temp = self.createCompletePublicationInfoTableFromDoi(pub_doi)
                df = pd.concat([df, df_temp])
            
            df = df.fillna(value="") #clean up NaN results

            return df
    

    def getPublicationsByAuthorId(self, id):
        with connect(self.dbPath) as con:
            #Obtain df publications ids/dois of publications written by this author
            df_aut_group = pd.read_sql("""
                                    SELECT PublicationDoi
                                    FROM AuthorGroup 
                                    WHERE AuthorId='%s';
                                    """ % id, con)

            #Obtain list of these ids/dois in order to iterate over and create complete publication info df
            doi_list = CreateListFromDataFrameColumn(df_aut_group, "PublicationDoi")
            df = DataFrame()
            for doi in doi_list:
                df_temp = self.createCompletePublicationInfoTableFromDoi(doi)
                df = pd.concat([df, df_temp])
            df = df.fillna(value="") #clean up NaN results

            return df
    
    def getMostCitedPublication(self):
        with connect(self.dbPath) as con:
            #From the Cites table get a 1 column df of cited publication dois
            cited_pubs_dois_df = pd.read_sql("""
                                            SELECT DoiOfCitedPubs
                                            FROM Cites;
                                            """, con)
            #Create a dictionary counting how many times each doi appears
            cites_dict = dict()
            for idx, row in cited_pubs_dois_df.iterrows():
                doi = row["DoiOfCitedPubs"]
                if doi != "[]":
                    if doi not in cites_dict:
                        cites_dict[doi] = 0
                    cites_dict[doi] += 1

            #Find max value/doi in the dictionary = most cited publication
            max_cite = max(cites_dict, key=cites_dict.get)

            #Create df of this publication's info
            df = self.createCompletePublicationInfoTableFromDoi(max_cite)
            df = df.fillna(value="") #clean up NaN results
            
            return df   

    
    def getMostCitedPublicationValue(self): #Supplementary method, based on above, to help with the GenericQueryProcessor 
        with connect(self.dbPath) as con:
            cited_pubs_dois_df = pd.read_sql("""
                                            SELECT DoiOfCitedPubs
                                            FROM Cites;
                                            """, con)
            cites_dict = dict()
            for idx, row in cited_pubs_dois_df.iterrows():
                doi = row["DoiOfCitedPubs"]
                if doi != "[]":
                    if doi not in cites_dict:
                        cites_dict[doi] = 0
                    cites_dict[doi] += 1
    
            max_cite_value = max(cites_dict.values())

            return max_cite_value
    
    def getMostCitedVenue(self):
        with connect(self.dbPath) as con:
            #Create a df of cited publication dois merged with venue ids, so we have duplicate VenueInternalIds which we can count
            df_Cites = pd.read_sql("""
                                    SELECT DoiOfCitedPubs
                                    from Cites;
                                    """, con)
            df_VenueId = pd.read_sql("""
                                    SELECT VenueInternalId, PublicationDoi
                                    FROM VenueId;
                                    """, con)
            df_VenueId = df_VenueId.drop_duplicates() #Remove initial VenueInternalId duplicates which are based on venues with 2 id values
            initial_merge_df = merge(df_Cites, df_VenueId, left_on="DoiOfCitedPubs", right_on="PublicationDoi")
            cited_venues_df = initial_merge_df[["VenueInternalId", "PublicationDoi"]]

            #Isolate the VenueInternalId column. Iterate on it and count occurences of each VenueInternalId, stored using a dictionary
            df_venues_internal_ids = cited_venues_df[["VenueInternalId"]]
            venue_dict = dict()
            for idx, row in df_venues_internal_ids.iterrows():
                venue = row["VenueInternalId"]
                if venue not in venue_dict:
                    venue_dict[venue] = 0
                venue_dict[venue] += 1
            max_value = max(venue_dict.values())
            max_venue = max(venue_dict, key=venue_dict.get)

            #Find one example publication id/doi contained in this Venue in order to call general info on the Venue
            example_doi = ""
            df_max_venue = pd.read_sql("""
                                        SELECT PublicationDoi
                                        FROM VenueId
                                        WHERE VenueInternalId='%s';
                                        """ % max_venue, con)
            for idx, row in df_max_venue.iterrows():
                example_doi = row["PublicationDoi"]
                break

            #Call the dfs of all Venue subclasses to locate general info on this Venue, depending on subclass
            venue_type = ""
            df_j = pd.read_sql("""
                                SELECT *
                                FROM Journal
                                WHERE id='%s';
                                """ % example_doi, con)
            if df_j.empty == False:
                venue_type == Journal
                df_venues = merge(cited_venues_df, df_j, left_on="PublicationDoi", right_on="id")
                df_venues = df_venues.drop_duplicates(subset="VenueInternalId") #Eliminate duplicated InternalIds to get 1 row
                df_venues = df_venues[["VenueInternalId", "id", "VenueTitle", "Organization", "venueType"]]
            df_b = pd.read_sql("""
                                SELECT *
                                FROM Book
                                WHERE id='%s';
                                """ % example_doi, con)  
            if df_b.empty == False:
                venue_type == Book
                df_venues = merge(cited_venues_df, df_b, left_on="PublicationDoi", right_on="id")
                df_venues = df_venues.drop_duplicates(subset="VenueInternalId") #Eliminate duplicated InternalIds to get 1 row
                df_venues = df_venues[["VenueInternalId", "id", "VenueTitle", "Organization", "venueType"]]
            df_p = pd.read_sql("""
                                SELECT *
                                FROM Proceedings
                                WHERE id='%s';
                                """ % example_doi, con)
            if df_p.empty == False:
                venue_type == Proceedings
                df_venues = merge(cited_venues_df, df_p, left_on="PublicationDoi", right_on="id")
                df_venues = df_venues.drop_duplicates(subset="VenueInternalId") #Eliminate duplicated InternalIds to get 1 row
                df_venues = df_venues[["VenueInternalId", "id", "VenueTitle", "Organization", "event", "venueType"]]

            df = df_venues.fillna(value="") #clean up NaN results

            return df
            
    
    def getMostCitedVenueValue(self): #Supplementary method, based on the above, to help with GenericQueryProcessor step
        with connect(self.dbPath) as con:
            df_Cites = pd.read_sql("""
                                    SELECT DoiOfCitedPubs
                                    from Cites;
                                    """, con)
            df_VenueId = pd.read_sql("""
                                    SELECT VenueInternalId, PublicationDoi
                                    FROM VenueId;
                                    """, con)
            initial_merge_df = merge(df_Cites, df_VenueId, left_on="DoiOfCitedPubs", right_on="PublicationDoi")
            df_venues_internal_ids = initial_merge_df[["VenueInternalId"]]
        
            venue_dict = dict()
            for idx, row in df_venues_internal_ids.iterrows():
                venue = row["VenueInternalId"]
                if venue not in venue_dict:
                    venue_dict[venue] = 0
                venue_dict[venue] += 1
            max_value = max(venue_dict.values())

            return max_value
   

    def getVenuesByPublisherId(self, id):
        with connect(self.dbPath) as con:
            #Create a df of general info for all Venue subclasses with this PublisherId
            df_j = pd.read_sql("""
                                SELECT *
                                FROM Journal
                                WHERE Organization='%s';
                                """ % id, con)
            df_b = pd.read_sql("""
                               SELECT *
                               FROM Book
                               WHERE Organization='%s';
                               """ % id, con)
            df_p = pd.read_sql("""
                                SELECT *
                                FROM Proceedings
                                WHERE Organization='%s';
                                """ % id, con)
            df = pd.concat([df_j, df_b, df_p])

            #Call the VenueId table in order to merge general info df with VenueInternalId
            VenueId = pd.read_sql("""
                                SELECT VenueInternalId, PublicationDoi
                                FROM VenueId;
                                """, con)
            df = merge(df, VenueId, left_on="id", right_on="PublicationDoi")
            df = df[["VenueInternalId", "VenueTitle", "Organization", "event", "venueType"]]
            df = df.drop_duplicates(subset="VenueInternalId") #since some venues have more than 1 id (issn/isbn), duplicates can occur
            df = df.fillna(value="") #clean up NaN results

            return df
    
    def getPublicationInVenue(self, id):
        with connect(self.dbPath) as con:
            #Obtain 1 column df of publication ids/dois matching the specified venue id
            df_venueid = pd.read_sql("""
                                    SELECT PublicationDoi
                                    FROM VenueId
                                    WHERE VenueId='%s';
                                    """ % id, con)
            
            #Get a list of publication dois and use to create df of full publication info for each publication
            doi_list = CreateListFromDataFrameColumn(df_venueid, "PublicationDoi")
            df = DataFrame()
            for pub_doi in doi_list:
                df_temp = self.createCompletePublicationInfoTableFromDoi(pub_doi)
                df = pd.concat([df, df_temp])
            df = df.fillna(value="") #clean up NaN results

            return df

    
    def getJournalArticlesInIssue(self, issue, volume, id):
        return self.getJournalArticle(id, volume, issue)
        
    
    def getJournalArticlesInVolume(self, volume, id):
        return self.getJournalArticle(id, volume)
        
        
    def getJournalArticlesInJournal(self, id):
        return self.getJournalArticle(id)
    
    def getProceedingsByEvent(self, event_partial_name):
        with connect(self.dbPath) as con:
            #Call all general info on Proceedings
            df_p = pd.read_sql("""
                                SELECT *
                                FROM Proceedings;
                                """, con)
            
            #Iterate over this df to check if the event partial name occurs in the event cell of each row
            df_global = DataFrame() #if 1+ proceedings paper has same event, concat them all
            for idx, row in df_p.iterrows():
                for item_idx, item in row.iteritems():
                    if item_idx == "event" and event_partial_name in item:
                        df_local = DataFrame(df_p.loc[idx])
                        df_local = df_local.transpose()
                        df_global = pd.concat([df_global, df_local]) 

            #Add VenueInternalId to general info Proceedings df
            df = DataFrame()
            df_venues = pd.read_sql("""
                                    SELECT VenueInternalId, PublicationDoi
                                    FROM VenueId;
                                    """, con) 
            df_venues = df_venues.drop_duplicates(subset=["VenueInternalId"]) #Eliminate repeated VenueInternalIds based on venues with multi-valued id
            df = merge(df_global, df_venues, left_on="id", right_on="PublicationDoi")
            df = df[["VenueInternalId", "VenueTitle", "Organization", "event"]]
            df = df.fillna(value="") #clean up NaN results

            return df
      
              
    def getPublicationAuthors(self, id):
        with connect(self.dbPath) as con:
            #Obtain info from AuthorGroup table of authors matched with the publication doi
            aut_group_df = pd.read_sql("""
                                        SELECT AutGroupInternalId, AuthorId, givenName, familyName
                                        FROM AuthorGroup
                                        WHERE PublicationDoi='%s';
                                        """ % id, con)
            #Use separate Person table to combine general author info with PersonInternalId
            person_df = pd.read_sql("""
                                    SELECT PersonInternalId, PersonId
                                    FROM Person;
                                    """, con)
            
            df = merge(aut_group_df, person_df, left_on="AuthorId", right_on="PersonId")
            df = df[["PersonInternalId", "AutGroupInternalId", "PersonId", "givenName", "familyName"]]
            df = df.fillna(value="") #clean up NaN results

            return df
    

    def getPublicationsByAuthorName(self, aut_partial_name):
        with connect(self.dbPath) as con:
            #Convert aut_partial_name to all lowercase to maximise results
            aut_partial_name = aut_partial_name.lower()
            #Call the entire AuthorGroup table
            df_aut_group = pd.read_sql("""
                                        SELECT *
                                        FROM AuthorGroup;
                                        """, con)
            #Organise full names into a complete string, insert in a tuple with author id, save tuple into a list
            list_of_author_tuples = []
            first_name = ""
            second_name = ""
            full_name = ""
            for idx, row in df_aut_group.iterrows():
                first_name = row["givenName"]
                second_name = row["familyName"]
                full_name = first_name + " " + second_name
                author_id = row["AuthorId"]
                author_info_tuple = (full_name, author_id)
                list_of_author_tuples.append(author_info_tuple)
            
            #Search through the first item in these tuples for the aut_partial_name. If present, append AuthorId to list
            author_id_list = []
            for author_info_tuple in list_of_author_tuples:
                if aut_partial_name in author_info_tuple[0]:
                    author_id_list.append(author_info_tuple[1])
            
            #Use identified AuthorIds to isolate publication ids/dois written by that author
            df_dois = DataFrame()
            for iden in author_id_list:
                author_info_df = pd.read_sql("""
                                            SELECT PublicationDoi
                                            FROM AuthorGroup
                                            WHERE AuthorId='%s';
                                            """ % iden, con)
                df_dois = pd.concat([df_dois, author_info_df])
            
            #Create a list of publication dois/ids and use it to get df of full publication info
            doi_list = CreateListFromDataFrameColumn(df_dois, "PublicationDoi")
            df = DataFrame()
            for pub_doi in doi_list:
                df_temp = self.createCompletePublicationInfoTableFromDoi(pub_doi)
                df = pd.concat([df, df_temp])
            df = df.fillna(value="") #clean up NaN results

            return df


    def getDistinctPublishersOfPublications(self, list_publications_ids):
        with connect(self.dbPath) as con:
            #Call all Organization information
            publishers_df = pd.read_sql("""
                                        SELECT *
                                        FROM Organization;
                                        """, con)

            #Iterate over publications ids list, searching through Venues for the appropriate publishers/Organizations
            df = DataFrame()       
            for id in list_publications_ids:
                df_j = pd.read_sql("""
                                SELECT Organization
                                FROM Journal
                                WHERE id='%s';
                                """ % id, con)
                df_b = pd.read_sql("""
                               SELECT Organization
                               FROM Book
                               WHERE id='%s';
                               """ % id, con)
                df_p = pd.read_sql("""
                                SELECT Organization
                                FROM Proceedings
                                WHERE id='%s';
                                """ % id, con)
                df = pd.concat([df, df_j, df_b, df_p])
            
            #Remove duplicate Organizations from the df to make sure all publishers are distinct
            df = df.drop_duplicates()
    
            #Merge this 1 column df containing publisher ids with the general info Organization df
            df = merge(df, publishers_df, left_on="Organization", right_on="PublisherId")
            df = df[["PublisherInternalId", "PublisherId", "PublisherName"]]
            df = df.fillna(value="") #clean up NaN results

            return df


####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
#TRIPLESTORE PROCESSORS

class TriplestoreProcessor(object):
    def __init__(self):
        self.endpointUrl = ""

    def getEndpointUrl(self):
        return self.endpointUrl

    def setEndpointUrl(self, newEndpointUrl):
        result = True
        if newEndpointUrl != self.endpointUrl:
            self.endpointUrl = newEndpointUrl
        else:
            result = False
        return result


# classes of resources
journalarticle = URIRef("https://schema.org/ScholarlyArticle")
bookchapter = URIRef("https://schema.org/Chapter")
journal = URIRef("https://schema.org/Periodical")
book = URIRef("https://schema.org/Book")
proceedings = URIRef("https://schema.org/Legislation")
proceedingspaper = URIRef("https://schema.org/Article")

# attributes related to classes
givenName = URIRef("https://schema.org/givenName")
familyName = URIRef("https://schema.org/familyName")
identifier = URIRef("https://schema.org/identifier")
publicationYear = URIRef("https://schema.org/datePublished")
title = URIRef("https://schema.org/name")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
chapterNumber = URIRef("https://schema.org/Chapter")
title = URIRef("https://schema.org/name")
name = URIRef("https://schema.org/name")
location = URIRef("https://schema.org/location")
event = URIRef("https://schema.org/Event")


# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")
publisher = URIRef("https://schema.org/publishedBy")
cites = URIRef("https://schema.org/mentions")
author = URIRef("https://schema.org/author")
publication = URIRef("https://schema.org/publication")


class TriplestoreDataProcessor(TriplestoreProcessor):
    pass
    venue_internal_id = dict()
    authors_internal_id = dict()
    references_internal_id = dict()

    def uploadData(self, path):
        my_graph = Graph()
        self.path = path
        base_url = "https://github.com/data-science-project-"
        if ".json" in self.path:
            other_data = read_json(self.path, orient="record")
            # authors:
            authors = other_data.get('authors')
            author_idx = 0

            for key, value in authors.iteritems():
                if "doi" in key:
                    for item in value:
                        author_idx += 1
                        local_id = "author"+str(author_idx)
                        subj = URIRef(base_url + local_id)
                        my_graph.add(
                            (subj, givenName, Literal(item.get('given'))))
                        my_graph.add(
                            (subj, familyName, Literal(item.get('family'))))
                        my_graph.add(
                            (subj, identifier, Literal(item.get('orcid'))))
                        my_graph.add((subj, publication, Literal(key)))
                        TriplestoreDataProcessor.authors_internal_id[key] = subj
            # venue_id
            venues_id = other_data.get("venues_id")
            venue_idx = 0

            for key, value in venues_id.iteritems():
                if "doi" in key:
                    venue_idx += 1
                    local_id = "venue"+str(venue_idx)
                    subj = URIRef(base_url + local_id)
                    if not value:
                        value = "not available"
                        my_graph.add((subj, publication, Literal(key)))
                        my_graph.add((subj, identifier, Literal(value)))

                    else:
                        newValue = pd.Series(value)
                        if pd.isna(newValue).all():
                            value = "not available"
                            my_graph.add((subj, publication, Literal(key)))
                            my_graph.add((subj, identifier, Literal(value)))
                        else:
                            for item in value:
                                if pd.isna(item):
                                    item = "not available"
                                my_graph.add((subj, publication, Literal(key)))
                                my_graph.add((subj, identifier, Literal(item)))
                    TriplestoreDataProcessor.venue_internal_id[key] = subj

            # references
            references = other_data.get("references")
            reference_idx = 0
            for key, value in references.iteritems():
                if "doi" in key:
                    reference_idx += 1
                    local_id = "reference"+str(reference_idx)
                    subj = URIRef(base_url + local_id)
                    if not value:
                        value = "not available"
                        my_graph.add((subj, publication, Literal(key)))
                        my_graph.add((subj, identifier, Literal(value)))
                    else:
                        newValue = pd.Series(value)
                        if pd.isna(newValue).all():
                            value = "not available"
                            my_graph.add((subj, publication, Literal(key)))
                            my_graph.add((subj, identifier, Literal(value)))
                        else:
                            for item in value:
                                if pd.isna(item):
                                    item = "not available"
                                my_graph.add((subj, publication, Literal(key)))
                                my_graph.add((subj, identifier, Literal(item)))
                    TriplestoreDataProcessor.references_internal_id[key] = subj

            # Publishers:
            publishers = other_data.get('publishers')
            publisher_idx = 0
            for key, value in publishers.iteritems():
                if "crossref" in key:
                    for item in value:
                        publisher_idx += 1
                        local_id = "publisher"+str(publisher_idx)
                        subj = URIRef(base_url + local_id)
                        my_graph.add((subj, publisher, Literal(key)))
                        my_graph.add((subj, identifier, Literal(value['id'])))
                        my_graph.add((subj, name, Literal(value['name'])))
            # Upload to database:

            store = SPARQLUpdateStore()

            store.open((self.endpointUrl, self.endpointUrl))
            for triple in my_graph.triples((None, None, None)):
                store.add(triple)
            return True

        elif ".csv" in self.path:

            publications = read_csv(self.path, keep_default_na=False, dtype={
                "id": "string",
                "title": "string",
                "publication_year": "int",
                "publication_venue": "string",
                "type": "string",
                "issue": "string",
                "volume": "string",
                "chapter": "string",
                "venue_type": "string",
                "publisher": "string"
            })

            # Publications:
            for idx, row in publications.iterrows():
                local_id = "publication-" + str(idx)

                subj = URIRef(base_url + local_id)

                if row["type"] == "journal-article":
                    my_graph.add((subj, RDF.type, journalarticle))
                    if not(row["issue"]):
                        row["issue"] = "not available"
                    if pd.isna((row["issue"])):
                        row["issue"] = "not available"
                    my_graph.add((subj, issue, Literal(row["issue"])))
                    if not(row["volume"]):
                        row["volume"] = "not available"
                    if pd.isna(row["volume"]):
                        row["volume"] = "not available"
                    my_graph.add((subj, volume, Literal(row["volume"])))
                elif row["type"] == "book-chapter":
                    my_graph.add((subj, RDF.type, bookchapter))
                    if not(row["chapter"]):
                        row["chapter"] = "not available"
                    if pd.isna(row["chapter"]):
                        row["chapter"] = "not available"
                    my_graph.add(
                        (subj, chapterNumber, Literal(row["chapter"])))
                elif row["type"] == "proceedings":
                    my_graph.add((subj, RDF.type, proceedingspaper))
                    if not(row["event"]):
                        row["event"] = "not available"
                    if pd.isna(row["event"]):
                        row["event"] = "not available"
                    my_graph.add((subj, event, Literal(row["event"])))

                my_graph.add((subj, name, Literal(row["title"])))
                my_graph.add((subj, identifier, Literal(row["id"])))
                my_graph.add((subj, publicationYear, Literal(
                    str(row["publication_year"]))))
                my_graph.add((subj, location, Literal(row["venue_type"])))
                my_graph.add((subj, publisher, Literal(row["publisher"])))

                if TriplestoreDataProcessor.authors_internal_id != {}:
                    if TriplestoreDataProcessor.authors_internal_id[row["id"]]:
                        my_graph.add(
                            (subj, author, TriplestoreDataProcessor.authors_internal_id[row["id"]]))
                if TriplestoreDataProcessor.venue_internal_id != {}:
                    if TriplestoreDataProcessor.venue_internal_id[row["id"]]:
                        my_graph.add(
                            (subj, publicationVenue, TriplestoreDataProcessor.venue_internal_id[row["id"]]))
                if TriplestoreDataProcessor.references_internal_id != {}:
                    if TriplestoreDataProcessor.references_internal_id[row["id"]]:
                        my_graph.add(
                            (subj, cites, TriplestoreDataProcessor.references_internal_id[row["id"]]))

        # Upload to database:

            store = SPARQLUpdateStore()

            store.open((self.endpointUrl, self.endpointUrl))
            for triple in my_graph.triples((None, None, None)):
                store.add(triple)
            return True

        else:
            return False


class QueryProcessor():
    pass


class TriplestoreQueryProcessor(TriplestoreProcessor, QueryProcessor):
    def __init__(self):
        super().__init__()
        super(TriplestoreProcessor, self).__init__()

    def getPublicationsPublishedInYear(self, year):
        self.year = year
        # get publication with id, name, date
        publication_query = """
                        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX schema: <https://schema.org/>
                        SELECT ?publication_name ?publication_id ?publication_date
                        WHERE {{            
                                ?s schema:datePublished "{}".
                                ?s schema:datePublished ?publication_date.
                                ?s schema:name ?publication_name.
                                ?s schema:identifier ?publication_id
                        }}
                        """.format(self.year)
        df_pub = get(self.endpointUrl, publication_query, True)
        # change publication ids to string
        list1 = df_pub["publication_id"].to_list()
        string1 = '("{0}")'.format('") ("'.join(list1))
        # get authors
        author_query = """
                            PREFIX schema: <https://schema.org/>
                                    SELECT ?givenName ?familyName ?author_id
                                    WHERE {{
                                        VALUES (?publication_id){{{}}}
                                            ?s schema:publication ?publication_id.
                                            ?s schema:identifier ?author_id.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:familyName ?familyName

                                    }}

                                    """.format(string1)

        df_author = get(self.endpointUrl, author_query, True)
        # get cited pubs
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string1)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)
        # get venues in one dataframe
        publisher_id_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publisher_id
                                WHERE {{           
                                        VALUES (?publication_id){{{}}}
                                        ?s schema:publishedBy ?publisher_id.
                                        ?s schema:identifier ?publication_id.
                                }}
                                """.format(string1)
        df_sparql3 = get(self.endpointUrl, publisher_id_query, True)
        venue_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publication ?venue_id
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:identifier ?venue_id.
                                        ?s schema:publication ?publication.
                                        FILTER( contains(str(?s), "venue") )
                                }}  

                                """.format(string1)
        df_sparql4 = get(self.endpointUrl, venue_query, True)
        df_venue = pd.concat(
            [df_sparql3, df_sparql4.venue_id], axis=1, join='inner')
        # combine dataframe
        df_final = pd.concat(
            [df_pub, df_author, df_cited_pub, df_venue], axis=1, join='inner')

        return df_final

    def getPublicationsByAuthorId(self, authorID):
        self.authorID = authorID
        author_query = """
                            PREFIX schema: <https://schema.org/>
                                    SELECT DISTINCT ?publication_id ?givenName ?familyName ?author_id
                                    WHERE {{

                                            ?s schema:publication ?publication_id.
                                            ?s schema:identifier "{}".
                                            ?s schema:identifier ?author_id.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:familyName ?familyName

                                    }}

                                    """.format(self.authorID)

        df_sparql = get(self.endpointUrl, author_query, True)
        list1 = df_sparql["publication_id"].to_list()
        string1 = '("{0}")'.format('") ("'.join(list1))
        publication_query = """
                                    PREFIX schema: <https://schema.org/>
                                    SELECT DISTINCT ?publication_name ?publication_id ?publication_date
                                    WHERE {{
                                          VALUES (?publication_id){{{}}}
                                            ?s schema:identifier ?publication_id.
                                            ?s schema:name ?publication_name.
                                            ?s schema:datePublished ?publication_date   
                                    }}  

                                    """.format(string1)
        df_sparql1 = get(self.endpointUrl, publication_query, True)
        # get publication_year, name, id, author in one dataframe
        df_pub = pd.concat([df_sparql, df_sparql1.publication_name,
                           df_sparql1.publication_date], axis=1, join='inner')

        # get cited publication in one dataframe
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string1)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)

        # get venues in one dataframe
        publisher_id_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publisher_id
                                WHERE {{           
                                        VALUES (?publication_id){{{}}}
                                        ?s schema:publishedBy ?publisher_id.
                                        ?s schema:identifier ?publication_id.
                                }}
                                """.format(string1)
        df_sparql3 = get(self.endpointUrl, publisher_id_query, True)
        venue_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publication ?venue_id
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:identifier ?venue_id.
                                        ?s schema:publication ?publication.
                                        FILTER( contains(str(?s), "venue") )
                                }}  

                                """.format(string1)
        df_sparql4 = get(self.endpointUrl, venue_query, True)
        df_venue = pd.concat(
            [df_sparql3, df_sparql4.venue_id], axis=1, join='inner')
        # combine dataframe
        df_final = pd.concat(
            [df_pub, df_cited_pub, df_venue], axis=1, join='inner')

        return df_final

    def getMostCitedPublicationValue(self):
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?identifier
                                WHERE {
                                        ?s schema:identifier ?identifier.
                                       FILTER(( contains(str(?s), "reference") )&&(?identifier != "['not available']" )&&(?identifier != "not available" ))  


                                }  
                                """
        df_sparql = get(self.endpointUrl, cited_publication_query, True)
        # get pub ID
        most_cited_value = df_sparql['identifier'].value_counts().max()
        return most_cited_value

    def getMostCitedPublication(self):
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?identifier
                                WHERE {
                                        ?s schema:identifier ?identifier.
                                       FILTER(( contains(str(?s), "reference") )&&(?identifier != "['not available']" )&&(?identifier != "not available" ))  


                                }  
                                """
        df_sparql = get(self.endpointUrl, cited_publication_query, True)
        # get pub ID
        string = df_sparql['identifier'].value_counts().idxmax()
        # get pub name, date, id
        publication_query = """
                                PREFIX schema: <https://schema.org/>
                                SELECT   ?publication_name ?publication_id ?publication_date
                                WHERE {{          
                                       VALUES (?publication_id){{("{}")}}
                                        ?s schema:name ?publication_name.
                                        ?s schema:identifier ?publication_id.
                                        ?s schema:datePublished ?publication_date.
                                        FILTER( contains(str(?s), "publication") )

                                }}
                                """.format(string)
        df_pub = get(self.endpointUrl, publication_query, True)
        # get author
        author_query = """
                            PREFIX schema: <https://schema.org/>
                                    SELECT ?givenName ?familyName ?author_id
                                    WHERE {{
                                        VALUES (?publication_id){{("{}")}}
                                            ?s schema:publication ?publication_id.
                                            ?s schema:identifier ?author_id.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:familyName ?familyName.

                                    }}

                                    """.format(string)

        df_author = get(self.endpointUrl, author_query, True)
        # get cited pubs
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{("{}")}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)
        # get venue
        publisher_id_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publisher_id
                                WHERE {{           
                                        VALUES (?publication){{("{}")}}
                                        ?s schema:publishedBy ?publisher_id.
                                        ?s schema:identifier ?publication_id.
                                }}
                                """.format(string)
        df_sparql3 = get(self.endpointUrl, publisher_id_query, True)
        venue_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publication ?venue_id
                                WHERE {{
                                        VALUES (?publication){{("{}")}}
                                        ?s schema:identifier ?venue_id.
                                        ?s schema:publication ?publication.
                                        FILTER( contains(str(?s), "venue") )
                                }}  

                                """.format(string)
        df_sparql4 = get(self.endpointUrl, venue_query, True)
        df_venue = pd.concat(
            [df_sparql3, df_sparql4.venue_id], axis=1, join='inner')
        df_final = pd.concat(
            [df_pub, df_cited_pub, df_venue, df_author], axis=1, join='inner')

        return df_final

    def getMostCitedVenue(self):
        cited_publication_query = """

                                    PREFIX schema: <https://schema.org/>
                                    SELECT ?identifier
                                    WHERE {
                                            ?s schema:identifier ?identifier.
                                           FILTER(( contains(str(?s), "reference") )&&(?identifier != "['not available']" )&&(?identifier != "not available" ))  


                                    }  
                                    """
        df_sparql = get(self.endpointUrl, cited_publication_query, True)
        string = df_sparql['identifier'].value_counts().idxmax()
        publisher_id_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publisher_id 
                                WHERE {{           
                                        VALUES (?publication_id){{("{}")}}
                                        ?s schema:publishedBy ?publisher_id.
                                        ?s schema:identifier ?publication_id.
                                }}
                                """.format(string)
        df_sparql1 = get(self.endpointUrl, publisher_id_query, True)
        most_cited_venue_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publication_id ?venue_id
                                WHERE {{
                                        VALUES (?publication){{("{}")}}
                                        ?s schema:identifier ?venue_id.
                                        ?s schema:publication ?publication_id.
                                        FILTER( contains(str(?s), "venue") )
                                }}  

                                """.format(string)
        df_sparql2 = get(self.endpointUrl, most_cited_venue_query, True)
        df_final = pd.concat([df_sparql1, df_sparql2], axis=1, join='inner')
        return df_final

    def getMostCitedVenueValue(self):
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?identifier
                                WHERE {
                                        ?s schema:identifier ?identifier.
                                       FILTER(( contains(str(?s), "reference") )&&(?identifier != "['not available']" )&&(?identifier != "not available" ))  


                                }  
                                """
        df_sparql = get(self.endpointUrl, cited_publication_query, True)
        # get pub ID
        most_cited_value = df_sparql['identifier'].value_counts().max()
        return most_cited_value

    def getVenuesByPublisherId(self, publisherID):
        self.publisherID = publisherID

        publication_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT DISTINCT   ?publication_id ?publisher_id
                            WHERE {{          
                                   VALUES (?publisher_id){{("{}")}}
                                    ?s schema:publishedBy ?publisher_id.
                                    ?s schema:identifier ?publication_id.
                                    FILTER( contains(str(?s), "publication") )

                            }}
                            """.format(self.publisherID)
        df_sparql = get(self.endpointUrl, publication_query, True)
        list1 = df_sparql["publication_id"].to_list()
        string1 = '("{0}")'.format('") ("'.join(list1))

        venue_query = """
                            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                            PREFIX schema: <https://schema.org/>
                            SELECT DISTINCT ?venue_id
                            WHERE {{
                                    VALUES (?publication){{{}}}
                                    ?s schema:publication ?publication.
                                    ?s schema:identifier ?venue_id.
                                    FILTER( contains(str(?s), "venue") )
                            }}
                            """.format(string1)
        df_sparql1 = get(self.endpointUrl, venue_query, True)
        df_final = pd.concat([df_sparql, df_sparql1], axis=1)
        df_final.fillna(method='ffill', inplace=True)
        #listofVenues=[(Venue(row.publication_id,row.venue_id,row.publisher_id)) for index, row in df_final.iterrows()][0]
        return df_final

    def getPublicationInVenue(self, venueID):
        self.venueID = venueID
        venue_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publication_id ?venue_id
                                WHERE {{
                                        VALUES (?venue_id){{("{}")}}
                                        ?s schema:identifier ?venue_id.
                                        ?s schema:publication ?publication_id.

                                }}  

                                """.format(self.venueID)
        df_sparql1 = get(self.endpointUrl, venue_query, True)
        list1 = df_sparql1["publication_id"].to_list()
        # get string of pub id
        string1 = '("{0}")'.format('") ("'.join(list1))
        publisher_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT DISTINCT ?publisher_id 
                            WHERE {{          
                                   VALUES (?identifier){{{}}}
                                    ?s schema:publishedBy ?publisher_id.
                                    ?s schema:identifier ?identifier.
                                    FILTER( contains(str(?s), "publication") )

                            }}
                            """.format(string1)
        df_sparql2 = get(self.endpointUrl, publisher_query, True)
        # get venue
        df_venue = pd.concat(
            [df_sparql1.venue_id, df_sparql2.publisher_id], axis=1, join='inner')

        # get pub name, date, identifier
        publication_query = """
                                PREFIX schema: <https://schema.org/>
                                SELECT   ?publication_id ?publication_name ?publication_date
                                WHERE {{          
                                       VALUES (?identifier){{{}}}
                                        ?s schema:name ?publication_name.
                                        ?s schema:identifier ?publication_id.
                                        ?s schema:datePublished ?publication_date
                                        FILTER( contains(str(?s), "publication") )

                                }}
                                """.format(string1)
        df_pub = get(self.endpointUrl, publication_query, True)
        # get authors
        author_query = """
                            PREFIX schema: <https://schema.org/>
                                    SELECT ?givenName ?familyName ?author_id
                                    WHERE {{
                                        VALUES (?publication_id){{{}}}
                                            ?s schema:publication ?publication_id.
                                            ?s schema:identifier ?author_id.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:familyName ?familyName

                                    }}

                                    """.format(string1)

        df_author = get(self.endpointUrl, author_query, True)
        # get cited publication in one dataframe
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string1)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)
        df_final = pd.concat(
            [df_venue, df_pub, df_author, df_cited_pub], axis=1, join='inner')

        return df_final

    def getJournalArticlesInIssue(self, venueID, volume, issue):
        self.venueID = venueID
        self.volume = volume
        self.issue = issue
        publication_query = """
                                        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                        PREFIX schema: <https://schema.org/>
                                        SELECT DISTINCT ?publication_id ?venue_id
                                        WHERE {{
                                                VALUES (?venue_id){{("{}")}}
                                                ?s schema:identifier ?venue_id.
                                                ?s schema:publication ?publication_id.
                                                FILTER ( contains(str(?s), "venue") )

                                        }}  

                                        """.format(self.venueID)
        df_sparql = get(self.endpointUrl, publication_query, True)
        list1 = df_sparql["publication_id"].to_list()
        # get string of pub IDs
        string1 = '("{0}")'.format('") ("'.join(list1))
        publisher_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT DISTINCT ?publisher_id 
                            WHERE {{          
                                   VALUES (?identifier){{{}}}
                                    ?s schema:publishedBy ?publisher_id.
                                    ?s schema:identifier ?identifier.
                                    FILTER( contains(str(?s), "publication") )

                            }}
                            """.format(string1)
        df_sparql1 = get(self.endpointUrl, publisher_query, True)
        # get venue
        df_venue = pd.concat(
            [df_sparql.venue_id, df_sparql1.publisher_id], axis=1, join='inner')
        # get pub name pub id issue volume pub_date
        journal_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT ?publication_name ?issue ?volume ?publication_id ?publication_date
                                WHERE {{
                                        VALUES (?publication_id){{{}}}
                                        ?s schema:identifier ?publication_id.
                                        ?s rdf:type schema:ScholarlyArticle.
                                        ?s schema:name ?publication_name.
                                        ?s schema:issueNumber "{}".
                                        ?s schema:volumeNumber "{}".
                                        ?s schema:issueNumber ?issue.
                                        ?s schema:volumeNumber ?volume.
                                        ?s schema:datePublished ?publication_date.

                                }}  

                                """.format(string1, self.issue, self.volume)
        df_pub = get(self.endpointUrl, journal_query, True)
        # get authors
        author_query = """
                            PREFIX schema: <https://schema.org/>
                                    SELECT ?givenName ?familyName ?author_id
                                    WHERE {{
                                        VALUES (?publication_id){{{}}}
                                            ?s schema:publication ?publication_id.
                                            ?s schema:identifier ?author_id.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:familyName ?familyName.

                                    }}

                                    """.format(string1)

        df_author = get(self.endpointUrl, author_query, True)
        # get cited pubs
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string1)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)
        df_final = pd.concat(
            [df_pub, df_venue, df_author, df_cited_pub], axis=1, join='inner')

        return df_final

    def getJournalArticlesInVolume(self, venueID, volume):
        self.venueID = venueID
        self.volume = volume
        publication_query = """
                                    PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    SELECT DISTINCT ?publication_id ?venue_id
                                    WHERE {{
                                            VALUES (?venue_id){{("{}")}}
                                            ?s schema:identifier ?venue_id.
                                            ?s schema:publication ?publication_id.
                                            FILTER ( contains(str(?s), "venue") )

                                    }}  

                                    """.format(self.venueID)
        df_sparql = get(self.endpointUrl, publication_query, True)
        list1 = df_sparql["publication_id"].to_list()
        string1 = '("{0}")'.format('") ("'.join(list1))
        publisher_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT DISTINCT ?publisher_id 
                            WHERE {{          
                                   VALUES (?identifier){{{}}}
                                    ?s schema:publishedBy ?publisher_id.
                                    ?s schema:identifier ?identifier.
                                    FILTER( contains(str(?s), "publication") )

                            }}
                            """.format(string1)
        df_sparql1 = get(self.endpointUrl, publisher_query, True)
        # get venue
        df_venue = pd.concat(
            [df_sparql.venue_id, df_sparql1], axis=1, join='inner')

        # get pub issue volume name id
        journal_article_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT ?publication_id ?publication_name ?issue ?volume ?publication_date
                                WHERE {{
                                      VALUES (?publication_id ){{{}}}
                                        ?s schema:identifier ?publication_id.
                                        ?s schema:name ?publication_name.
                                        ?s rdf:type schema:ScholarlyArticle.
                                        ?s schema:volumeNumber "{}".
                                        ?s schema:issueNumber ?issue.
                                        ?s schema:volumeNumber ?volume.
                                        ?s schema:datePublished ?publication_date

                                }}  

                                """.format(string1, self.volume)
        df_pub = get(self.endpointUrl, journal_article_query, True)
        # get authors
        author_query = """
                            PREFIX schema: <https://schema.org/>
                                    SELECT ?givenName ?familyName ?author_id
                                    WHERE {{
                                        VALUES (?publication_id){{{}}}
                                            ?s schema:publication ?publication_id.
                                            ?s schema:identifier ?author_id.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:familyName ?familyName

                                    }}

                                    """.format(string1)

        df_author = get(self.endpointUrl, author_query, True)
        # get cited pubs
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string1)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)
        df_final = pd.concat(
            [df_pub, df_venue, df_cited_pub, df_author], axis=1)
        df_final.fillna(method='ffill', inplace=True)
        return df_final

    def getJournalArticlesInJournal(self, venueID):
        self.venueID = venueID
        publication_query = """
                                        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                        PREFIX schema: <https://schema.org/>
                                        SELECT DISTINCT ?publication_id ?venue_id
                                        WHERE {{
                                                VALUES (?venue_id){{("{}")}}
                                                ?s schema:identifier ?venue_id.
                                                ?s schema:publication ?publication_id.
                                                FILTER ( contains(str(?s), "venue") )

                                        }}  

                                        """.format(self.venueID)

        df_sparql = get(self.endpointUrl, publication_query, True)
        list1 = df_sparql["publication_id"].to_list()
        string1 = '("{0}")'.format('") ("'.join(list1))
        publisher_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT DISTINCT ?publisher_id 
                            WHERE {{          
                                   VALUES (?identifier){{{}}}
                                    ?s schema:publishedBy ?publisher_id.
                                    ?s schema:identifier ?identifier.
                                    FILTER( contains(str(?s), "publication") )

                            }}
                            """.format(string1)
        df_sparql1 = get(self.endpointUrl, publisher_query, True)
        # get venue
        df_venue = pd.concat(
            [df_sparql.venue_id, df_sparql1], axis=1, join='inner')
        journal_article_query = """
                                    PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    SELECT ?publication_name ?publication_id ?volume ?issue ?publication_date
                                    WHERE {{
                                          VALUES (?publication_id){{{}}}
                                            ?s schema:identifier ?publication_id.
                                            ?s schema:name ?publication_name.
                                            ?s rdf:type schema:ScholarlyArticle.
                                            ?s schema:volumeNumber ?volume.
                                            ?s schema:issueNumber ?issue.
                                            ?s schema:datePublished ?publication_date.

                                    }}  

                                    """.format(string1)
        df_pub = get(self.endpointUrl, journal_article_query, True)
        # get authors
        author_query = """
                            PREFIX schema: <https://schema.org/>
                                    SELECT ?givenName ?familyName ?author_id
                                    WHERE {{
                                        VALUES (?publication_id){{{}}}
                                            ?s schema:publication ?publication_id.
                                            ?s schema:identifier ?author_id.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:familyName ?familyName

                                    }}

                                    """.format(string1)

        df_author = get(self.endpointUrl, author_query, True)
        # get cited pubs
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string1)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)
        df_final = pd.concat(
            [df_pub, df_venue, df_author, df_cited_pub], axis=1, join='inner')

        return df_final

    def getProceedingsByEvent(self, event):
        self.event = event
        # get pub
        proceeding_query = """

                        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX schema: <https://schema.org/>
                        SELECT DISTINCT ?publication_name ?publication_id ?event ?publication_date 
                        WHERE {{ 
                                ?s schema:event ?event.
                                
                                ?s schema:identifier ?publication_id.
                                ?s schema:name ?publication_name.
                                ?s schema:datePublished ?publication_date.
                                FILTER (contains(?event,"{}"))
                        }}


                        """.format(self.event)
        df_pub = get(self.endpointUrl, proceeding_query, True)

        list1 = df_pub["publication_id"].to_list()
        string1 = '("{0}")'.format('") ("'.join(list1))
        # get authors
        author_query = """
                            PREFIX schema: <https://schema.org/>
                                    SELECT ?givenName ?familyName ?author_id
                                    WHERE {{
                                        VALUES (?publication_id){{{}}}
                                            ?s schema:publication ?publication_id.
                                            ?s schema:identifier ?author_id.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:familyName ?familyName

                                    }}

                                    """.format(string1)

        df_author = get(self.endpointUrl, author_query, True)
        # get venue
        publisher_id_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publisher_id
                                WHERE {{           
                                        VALUES (?publication_id){{{}}}
                                        ?s schema:publishedBy ?publisher_id.
                                        ?s schema:identifier ?publication_id.
                                }}
                                """.format(string1)
        df_sparql3 = get(self.endpointUrl, publisher_id_query, True)
        venue_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publication ?venue_id
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:identifier ?venue_id.
                                        ?s schema:publication ?publication.
                                        FILTER( contains(str(?s), "venue") )
                                }}  

                                """.format(string1)
        df_sparql4 = get(self.endpointUrl, venue_query, True)
        df_venue = pd.concat(
            [df_sparql3, df_sparql4.venue_id], axis=1, join='inner')
        # get cited pubs
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string1)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)
        df_final = pd.concat(
            [df_pub, df_author, df_cited_pub, df_venue], axis=1, join='inner')
        return df_final

    def getPublicationAuthors(self, publicationID):
        self.publicationID = publicationID
        author_query = """
                                    PREFIX schema: <https://schema.org/>
                                    SELECT DISTINCT ?givenName ?familyName
                                    WHERE {{
                                            ?s schema:familyName ?familyName.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:publication "{}".
                                    }}  

                                    """.format(self.publicationID)
        df_sparql = get(self.endpointUrl, author_query, True)
        publication_query = """
                                    
                                    PREFIX schema: <https://schema.org/>
                                    SELECT ?publication_name ?publication_id 
                                    WHERE {{
                                          VALUES (?publication_id){{("{}")}}
                                            ?s schema:identifier ?publication_id.
                                            ?s schema:name ?publication_name.
                                    }}  

                                    """.format(self.publicationID)
        df_sparql1 = get(self.endpointUrl, publication_query, True)
        df_final = pd.concat([df_sparql, df_sparql1], axis=1)
        df_final.fillna(method='ffill', inplace=True)
        return df_final

    def getPublicationsByAuthorName(self, authorName):
        self.authorName = authorName.lower()
        author_query = """
                                PREFIX schema: <https://schema.org/>
                                SELECT ?givenName ?familyName ?publication_id
                                WHERE {{
                                        ?s schema:publication ?publication_id. 
                                        ?s schema:identifier ?identifier.
                                        ?s schema:givenName ?givenName.
                                        ?s schema:familyName ?familyName.
                                        FILTER (( contains(lcase(?familyName), "{}"))||( contains(lcase(?givenName), "{}")))


                                }}

                                """.format(self.authorName, self.authorName)

        df_author = get(self.endpointUrl, author_query, True)
        list1 = df_author["publication_id"].to_list()
        string1 = '("{0}")'.format('") ("'.join(list1))
        # get publication
        publication_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?publication_name ?publication_date
                            WHERE {{          
                                   VALUES (?publication_id){{{}}}
                                    ?s schema:name ?publication_name.
                                    ?s schema:identifier ?publication_id.
                                    ?s schema:datePublished ?publication_date
                                    FILTER( contains(str(?s), "publication") )

                            }}
                            """.format(string1)
        df_pub = get(self.endpointUrl, publication_query, True)
        # get cited publication in one dataframe
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string1)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)

        # get venues in one dataframe
        publisher_id_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publisher_id
                                WHERE {{           
                                        VALUES (?publication_id){{{}}}
                                        ?s schema:publishedBy ?publisher_id.
                                        ?s schema:identifier ?publication_id.
                                }}
                                """.format(string1)
        df_sparql3 = get(self.endpointUrl, publisher_id_query, True)
        venue_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publication ?venue_id
                                WHERE {{
                                        VALUES (?publication){{{}}}
                                        ?s schema:identifier ?venue_id.
                                        ?s schema:publication ?publication.
                                        FILTER( contains(str(?s), "venue") )
                                }}  

                                """.format(string1)
        df_sparql4 = get(self.endpointUrl, venue_query, True)
        df_venue = pd.concat(
            [df_sparql3, df_sparql4.venue_id], axis=1, join='inner')
        df_final = pd.concat(
            [df_pub, df_author, df_cited_pub, df_venue], axis=1, join='inner')

        return df_final

    def getDistinctPublishersOfPublications(self, publicationID):
        self.publicationID = publicationID
        publisher_id_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT ?publisher_id ?publication_id
                                WHERE {{           
                                        VALUES (?publication_id){{("{}")}}
                                        ?s schema:publishedBy ?publisher_id.
                                        ?s schema:identifier ?publication_id.
                                }}
                                """.format(self.publicationID)

        df_sparql = get(self.endpointUrl, publisher_id_query, True)
        list1 = df_sparql["publisher_id"].to_list()
        string1 = '("{0}")'.format('") ("'.join(list1))

        venue_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publisher_name
                                WHERE {{            
                                        VALUES (?publisher_id) {{{}}}
                                        ?s schema:identifier ?publisher_id.
                                        ?s schema:name ?publisher_name.
                                }}
                                """.format(string1)
        df_sparql1 = get(self.endpointUrl, venue_query, True)
        df_final = pd.concat(
            [df_sparql.publication_id, df_sparql1.publisher_name], axis=1)
        df_final.fillna(method='ffill', inplace=True)
        #listofOrganizations=[(Organization(row.publication_id,row.publisher_name)) for index, row in df_final.iterrows() ]
        return df_final

    def createPublicationObjectGraph(self, doi):
        self.doi = doi
        publication_query = """
                        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX schema: <https://schema.org/>
                        SELECT ?publication_name ?publication_id ?publication_date ?publication_type ?volume ?issue ?chapter
                        WHERE {{            
                                ?s schema:datePublished ?publication_date.
                                ?s schema:name ?publication_name.
                                ?s schema:identifier ?publication_id.
                                ?s schema:identifier "{}".
                        OPTIONAL {{?s rdf:type ?publication_type.}}
                        OPTIONAL {{?s schema:volumeNumber ?volume.}}
                        OPTIONAL {{?s schema:issueNumber ?issue.}}
                        OPTIONAL {{?s schema:Chapter?chapter.}}
                        OPTIONAL {{?s schema:event ?event.}}
                        }}
                        """.format(self.doi)
        df_pub = get(self.endpointUrl, publication_query, True)
        # change publication ids to string
        list1 = df_pub["publication_id"].to_list()
        string1 = '("{0}")'.format('") ("'.join(list1))
        # get authors
        author_query = """
                            PREFIX schema: <https://schema.org/>
                                    SELECT ?givenName ?familyName ?author_id ?publication_id
                                    WHERE {{
                                        VALUES (?publication_id){{{}}}
                                            ?s schema:publication ?publication_id.
                                            ?s schema:identifier ?author_id.
                                            ?s schema:givenName ?givenName.
                                            ?s schema:familyName ?familyName.

                                    }}

                                    """.format(string1)

        df_author = get(self.endpointUrl, author_query, True)
        # get cited pubs
        cited_publication_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub ?publication_id
                                WHERE {{
                                        VALUES (?publication_id){{{}}}
                                        ?s schema:publication ?publication_id.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(string1)
        df_cited_pub = get(self.endpointUrl, cited_publication_query, True)

        # get venues in one dataframe
        publisher_id_query = """
                                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publisher_id
                                WHERE {{           
                                        VALUES (?publication_id){{{}}}
                                        ?s schema:publishedBy ?publisher_id.
                                        ?s schema:identifier ?publication_id.
                                }}
                                """.format(string1)
        df_sparql3 = get(self.endpointUrl, publisher_id_query, True)
        venue_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT DISTINCT ?publication_id ?venue_id
                                WHERE {{
                                        VALUES (?publication_id){{{}}}
                                        ?s schema:identifier ?venue_id.
                                        ?s schema:publication ?publication_id.
                                        FILTER( contains(str(?s), "venue") )
                                }}  

                                """.format(string1)
        df_sparql4 = get(self.endpointUrl, venue_query, True)
        df_venue = pd.concat([df_sparql3, df_sparql4], axis=1, join='inner')
        # combine dataframe
        list_cited = df_cited_pub["cited_pub"].to_list()
        pub_type = df_pub["publication_type"].to_list()
        if len(pub_type) != 0:
            if pub_type[0] == "https://schema.org/Chapter":
                if len(list_cited) == 0:
                    return [BookChapter({row.publication_id}, row.publication_name, {(Person(row.givenName, row.familyName, {row.publication_id})) for index, row in df_author.iterrows()}, [], row.publication_date,
                                        [(Venue({row.publication_id}, row.venue_id, row.publisher_id)) for index, row in df_venue.iterrows()][0], row.chapter) for index, row in df_pub.iterrows()][0]
                elif list_cited[0].startswith("doi") != True:

                    return [BookChapter({row.publication_id}, row.publication_name, {(Person(row.givenName, row.familyName, {row.publication_id})) for index, row in df_author.iterrows()}, [], row.publication_date,
                                        [(Venue({row.publication_id}, row.venue_id, row.publisher_id))
                                         for index, row in df_venue.iterrows()][0],
                                        row.chapter) for index, row in df_pub.iterrows()][0]

                else:
                    cited_obs = []
                    for item in list_cited:
                        obj = self.createPublicationObjectGraph(item)
                        cited_obs.append(obj)
                        list_cited.remove(item)
                    return [BookChapter({row.publication_id}, row.publication_name,

                                        {(Person(row.givenName, row.familyName, {row.publication_id}))
                                         for index, row in df_author.iterrows()},
                                        cited_obs,
                                        row.publication_date, [(Venue(
                                            {row.publication_id}, row.venue_id, row.publisher_id)) for index, row in df_venue.iterrows()][0],

                                        row.chapter)for index, row in df_pub.iterrows()][0]
            elif pub_type[0] == "https://schema.org/ScholarlyArticle":
                if len(list_cited) == 0:
                    return [JournalArticle({row.publication_id}, row.publication_name,

                                           {(Person(row.givenName, row.familyName, {row.publication_id}))
                                            for index, row in df_author.iterrows()},
                                           [],
                                           row.publication_date, [(Venue(
                                               {row.publication_id}, row.venue_id, row.publisher_id)) for index, row in df_venue.iterrows()][0],

                                           row.issue, row.volume)for index, row in df_pub.iterrows()][0]
                elif list_cited[0].startswith("doi") != True:

                    return [JournalArticle({row.publication_id}, row.publication_name,

                                           {(Person(row.givenName, row.familyName, {row.publication_id}))
                                            for index, row in df_author.iterrows()},
                                           [],
                                           row.publication_date, [(Venue(
                                               {row.publication_id}, row.venue_id, row.publisher_id)) for index, row in df_venue.iterrows()][0],

                                           row.issue, row.volume)for index, row in df_pub.iterrows()][0]

                else:
                    cited_obs = []
                    for item in list_cited:
                        obj = self.createPublicationObjectGraph(item)
                        cited_obs.append(obj)
                        list_cited.remove(item)
                    return [JournalArticle({row.publication_id}, row.publication_name,

                                           {(Person(row.givenName, row.familyName, {row.publication_id}))
                                            for index, row in df_author.iterrows()},
                                           cited_obs,
                                           row.publication_date, [(Venue(
                                               {row.publication_id}, row.venue_id, row.publisher_id)) for index, row in df_venue.iterrows()][0],

                                           row.issue, row.volume)for index, row in df_pub.iterrows()][0]
            elif pub_type[0] == "https://schema.org/Article":

                if len(list_cited) == 0:
                    return [ProceedingsPaper({row.publication_id}, row.publication_name,

                                             {(Person(row.givenName, row.familyName, {row.publication_id}))
                                              for index, row in df_author.iterrows()},
                                             [],
                                             row.publication_date,
                                             [(Venue({row.publication_id}, row.venue_id, row.publisher_id))
                                              for index, row in df_venue.iterrows()][0],

                                             )for index, row in df_pub.iterrows()][0]
                elif list_cited[0].startswith("doi") != True:
                    return [ProceedingsPaper({row.publication_id}, row.publication_name,

                                             {(Person(row.givenName, row.familyName, {row.publication_id}))
                                              for index, row in df_author.iterrows()},
                                             [],
                                             row.publication_date,
                                             [(Venue({row.publication_id}, row.venue_id, row.publisher_id))
                                              for index, row in df_venue.iterrows()][0],

                                             )for index, row in df_pub.iterrows()][0]

                else:
                    cited_obs = []
                    for item in list_cited:
                        obj = self.createPublicationObjectGraph(item)
                        cited_obs.append(obj)
                        list_cited.remove(item)
                    return [ProceedingsPaper({row.publication_id}, row.publication_name,

                                             {(Person(row.givenName, row.familyName, {row.publication_id}))
                                              for index, row in df_author.iterrows()},
                                             cited_obs,
                                             row.publication_date,
                                             [(Venue({row.publication_id}, row.venue_id, row.publisher_id))
                                              for index, row in df_venue.iterrows()][0],

                                             )for index, row in df_pub.iterrows()][0]
            else:
                if len(list_cited) == 0:
                    [Publication({row.publication_id}, row.publication_name,

                     {(Person(row.givenName, row.familyName, {row.publication_id}))
                      for index, row in df_author.iterrows()},
                        [],
                        row.publication_date,
                        [(Venue({row.publication_id}, row.venue_id, row.publisher_id))
                         for index, row in df_venue.iterrows()][0],

                    )for index, row in df_pub.iterrows()][0]
                elif list_cited[0].startswith("doi") != True:
                    return [Publication({row.publication_id}, row.publication_name,

                                        {(Person(row.givenName, row.familyName, {row.publication_id}))
                                         for index, row in df_author.iterrows()},
                                        [],
                                        row.publication_date,
                                        [(Venue({row.publication_id}, row.venue_id, row.publisher_id))
                                         for index, row in df_venue.iterrows()][0],

                                        )for index, row in df_pub.iterrows()][0]

                else:
                    cited_obs = []
                    for item in list_cited:
                        obj = self.createPublicationObjectGraph(item)
                        cited_obs.append(obj)
                        list_cited.remove(item)
                    return [Publication({row.publication_id}, row.publication_name,

                                        {(Person(row.givenName, row.familyName, {row.publication_id}))
                                         for index, row in df_author.iterrows()},
                                        cited_obs,
                                        row.publication_date,
                                        [(Venue({row.publication_id}, row.venue_id, row.publisher_id))
                                         for index, row in df_venue.iterrows()][0],

                                        )for index, row in df_pub.iterrows()][0]

    def getCitedPublications(self, doi):
        self.doi = doi
        cited_pub_id_query = """

                                PREFIX schema: <https://schema.org/>
                                SELECT ?cited_pub
                                WHERE {{
                                        VALUES (?publication){{("{}")}}
                                        ?s schema:publication ?publication.
                                        ?s schema:identifier ?cited_pub.
                                       FILTER(( contains(str(?s), "reference") ))  

                                }}  
                                """.format(self.doi)

        df_cited_pub_id = get(self.endpointUrl, cited_pub_id_query, True)
        return df_cited_pub_id


####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################
#GENERIC QUERY PROCESSOR

class GenericQueryProcessor(object):
    def __init__(self, queryProcessor=set()):
        self.queryProcessor = queryProcessor
    

    def cleanQueryProcessors(self):
        if len(self.queryProcessor) == 0:
            return True
        
        elif len(self.QueryProcessor) != 0:
            for processor in self.queryProcessor:
                self.queryProcessor.remove(processor)
            return True
        
        else:
            return False
    

    def addQueryProcessor(self, processor):
        if processor not in self.queryProcessor:
            self.queryProcessor.add(processor)
            return True
        
        else:
            return False
    

    def getPublicationsPublishedInYear(self, year):
        # Returns list[Publication]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                year = str(year)
                pubs_df_by_year = processor.getPublicationsPublishedInYear(year)
                doi_list = CreateListFromDataFrameColumn(pubs_df_by_year, "id")
                for doi in doi_list:
                    result.append(processor.createPublicationObjectFromDoiRelational(doi))

            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                year = str(year)
                pubs_df_by_year = processor.getPublicationsPublishedInYear(
                    year)
                doi_list = []
                for idx, row in pubs_df_by_year.iterrows():
                    pub_id = row["publication_id"]
                    doi_list.append(pub_id)

                for doi in doi_list:
                    result.append(processor.createPublicationObjectGraph(doi))

        return result

    
    def getPublicationsByAuthorId(self, id):
        #Returns list[Publication]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                pubs_by_author_df = processor.getPublicationsByAuthorId(id)
                doi_list = CreateListFromDataFrameColumn(pubs_by_author_df, "id")
                for doi in doi_list:
                    result.append(processor.createPublicationObjectFromDoiRelational(doi))

            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                pubs_by_author_df = processor.getPublicationsByAuthorId(id)
                doi_list = []
                for idx, row in pubs_by_author_df.iterrows():
                    pub_id = row["publication_id"]
                    doi_list.append(pub_id)

                for doi in doi_list:
                    result.append(processor.createPublicationObjectGraph(doi))

        return result


    def getMostCitedPublication(self):
        #Returns Publication
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                largest_cites_value_r = processor.getMostCitedPublicationValue()
                most_cited_pub_df = processor.getMostCitedPublication()
                most_cited_pub_doi = ""
                for idx, row in most_cited_pub_df.iterrows():
                    most_cited_pub_doi = row["id"]
                most_cited_pub_obj_r = processor.createPublicationObjectFromDoiRelational(most_cited_pub_doi) 

                if len(self.queryProcessor) == 1: #In case of testing data with 1 processor (relational)
                    return most_cited_pub_obj_r

            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                largest_cites_value_t = processor.getMostCitedPublicationValue()
                most_cited_pub = processor.getMostCitedPublication()
                most_cited_pub_id = most_cited_pub["publication_id"].to_list()
                most_cited_pub_obj_t = processor.createPublicationObjectGraph(
                    most_cited_pub_id[0])

                if len(self.queryProcessor) == 1: #In case of testing data with 1 processor (triplestore)
                    return most_cited_pub_obj_t
        
        if largest_cites_value_r > largest_cites_value_t:
            return most_cited_pub_obj_r
        elif largest_cites_value_t > largest_cites_value_r:
            return most_cited_pub_obj_t
        else: #i.e. if the cites values are equal - not in the diagram, but decided to return a list
            return list(most_cited_pub_obj_r, most_cited_pub_obj_t)      

    
    def getMostCitedVenue(self):
        #Returns Venue
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                largest_venue_cites_value_r = processor.getMostCitedVenueValue()
                #Gather info on the most cited venue
                venue_internal_id = ""
                venue_title = ""
                venue_publisher = ""
                venue_event = ""
                most_cited_venue_df = processor.getMostCitedVenue() 
                for idx, row in most_cited_venue_df.iterrows():
                    venue_internal_id = row["VenueInternalId"]
                    venue_title = row["VenueTitle"]
                    venue_publisher = row["Organization"]
                    venue_type = row["venueType"]
                    if venue_type == "proceedings":
                        venue_event = row["event"]
                
                #Create Organization object for the publisher
                publisher = processor.getOrganizationObjectFromPublisherId(venue_publisher)
                #Obtain list of venue ids
                venue_id_list = processor.getVenueIdListByVenueInternalId(venue_internal_id)

                most_cited_venue_obj_r = ""
                if venue_type == "journal":
                    most_cited_venue_obj_r = Journal(venue_id_list, venue_title, publisher)
                elif venue_type == "book":
                    most_cited_venue_obj_r = Book(venue_id_list, venue_title, publisher)
                else: #i.e. if venue_type == "proceedings"
                    most_vited_venue_obj_r = Proceedings(venue_id_list, venue_title, publisher, venue_event)

                if len(self.queryProcessor) == 1: #In case of testing data with 1 processor (relational)
                    return most_cited_venue_obj_r


            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                largest_venue_cites_value_t = processor.getMostCitedVenueValue()
                most_cited_venue_df = processor.getMostCitedVenue()
                most_cited_venue_obj_t = [(Venue({row.publication_id}, row.venue_id, row.publisher_id))
                                          for index, row in most_cited_venue_df.iterrows()][0]

                if len(self.queryProcessor) == 1: #In case of testing data with 1 processor (triplestore)
                    return most_cited_venue_obj_t

        if largest_venue_cites_value_r > largest_venue_cites_value_t:
            return most_cited_venue_obj_r
        elif largest_venue_cites_value_t > largest_venue_cites_value_r:
            return most_cited_venue_obj_t
        else: #i.e. if the cites values are equal - not in the diagram, but decided to return a list
            return list(most_cited_venue_obj_r, most_cited_venue_obj_t)   
    

    def getVenuesByPublisherId(self, id):
        #Returns list[Venue]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                #Create Organization object
                publisher = processor.getOrganizationObjectFromPublisherId(id)
                #Get Venue info 
                venue_title = ""
                venue_internal_id = ""
                venue_type = ""
                venue_event = ""
                venue_info = processor.getVenuesByPublisherId(id)
                for idx, row in venue_info.iterrows():
                    venue_internal_id = row["VenueInternalId"]
                    venue_title = row["VenueTitle"]
                    venue_id_list = processor.getVenueIdListByVenueInternalId(venue_internal_id)
                    venue_type = row["venueType"]
                    if venue_type == "proceedings":
                        venue_event = row["event"]
                        result.append(Proceedings(venue_id_list, venue_title, publisher, venue_event))
                    elif venue_type == "journal":
                        result.append(Journal(venue_id_list, venue_title, publisher))
                    else: #i.e. if venue_type == "proceedings"
                        result.append(Book(venue_id_list, venue_title, publisher))
                                     

            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                df_venue = processor.getVenuesByPublisherId(id)
                result.extend([(Venue({row.publication_id}, row.venue_id, row.publisher_id))
                              for index, row in df_venue.iterrows()])
        
        return result

    
    def getPublicationInVenue(self, venueId):
        #Returns list[Publication]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                pubs_in_ven_df = processor.getPublicationInVenue(venueId)
                doi_list = CreateListFromDataFrameColumn(pubs_in_ven_df, "id")
                for doi in doi_list:
                    result.append(processor.createPublicationObjectFromDoiRelational(doi))
            

            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                df_pubs = processor.getPublicationInVenue(venueId)
                doi_list = []
                for idx, row in df_pubs.iterrows():
                    pub_id = row["publication_id"]
                    doi_list.append(pub_id)
                for doi in doi_list:
                    result.append(processor.createPublicationObjectGraph(doi))

        return result
    
    def getJournalArticlesInIssue(self, issue, volume, journalId):
        #Returns list[JournalArticle]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                articles_in_issue_df = processor.getJournalArticlesInIssue(issue, volume, journalId)
                doi_list = CreateListFromDataFrameColumn(articles_in_issue_df, "id")
                for doi in doi_list:
                    result.append(processor.createPublicationObjectFromDoiRelational(doi))

            
            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                articles_in_venue_df = processor.getJournalArticlesInIssue(
                    journalId, volume, issue)
                doi_list = []
                for idx, row in articles_in_venue_df.iterrows():
                    pub_id = row["publication_id"]
                    doi_list.append(pub_id)

                for doi in doi_list:
                    result.append(processor.createPublicationObjectGraph(doi))
        
        for pub in result:
            if type(pub) != JournalArticle:
                result.remove(pub)

        return result 


    def getJournalArticlesInVolume(self, volume, journalId):
        #Returns list[JournalArticle]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                articles_in_volume_df = processor.getJournalArticlesInVolume(volume, journalId)
                doi_list = CreateListFromDataFrameColumn(articles_in_volume_df, "id")
                for doi in doi_list:
                    result.append(processor.createPublicationObjectFromDoiRelational(doi))

            
            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                articles_in_venue_df = processor.getJournalArticlesInVolume(
                    journalId, volume)
                doi_list = []
                for idx, row in articles_in_venue_df.iterrows():
                    pub_id = row["publication_id"]
                    doi_list.append(pub_id)

                for doi in doi_list:
                    result.append(processor.createPublicationObjectGraph(doi))

        for pub in result:
            if type(pub) != JournalArticle:
                result.remove(pub)

        return result 
 

    def getJournalArticlesInJournal(self, journalId):
        #Returns list[JournalArticle]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                articles_in_journal_df = processor.getJournalArticlesInJournal(journalId)
                doi_list = CreateListFromDataFrameColumn(articles_in_journal_df, "id")
                for doi in doi_list:
                    result.append(processor.createPublicationObjectFromDoiRelational(doi))
                

            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                articles_in_venue_df = processor.getJournalArticlesInJournal(
                    journalId)
                doi_list = []
                for idx, row in articles_in_venue_df.iterrows():
                    pub_id = row["publication_id"]
                    doi_list.append(pub_id)
                for doi in doi_list:
                    result.append(processor.createPublicationObjectGraph(doi))   
        
        for pub in result:
            if type(pub) != JournalArticle:
                result.remove(pub)

        return result 

    
    def getProceedingsByEvent(self, eventPartialName):
        #Returns list[Proceeding]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                proceedings_df = processor.getProceedingsByEvent(eventPartialName)
                publisher_id = ""
                venue_internal_id = ""
                venue_id_list = []
                venue_title = ""
                venue_event = ""
                for idx, row in proceedings_df.iterrows():
                    publisher_id = row["Organization"]
                    publisher = processor.getOrganizationObjectFromPublisherId(publisher_id)
                    venue_internal_id = row["VenueInternalId"]
                    venue_id_list = processor.getVenueIdListByVenueInternalId(venue_internal_id)
                    venue_title = row["VenueTitle"]
                    venue_event = row["event"]
                    result.append(Proceedings(venue_id_list, venue_title, publisher, venue_event))


            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                proceedings_df = processor.getProceedingsByEvent(
                    eventPartialName)
                doi_list = []
                for idx, row in proceedings_df.iterrows():
                    pub_id = row["publication_id"]
                    doi_list.append(pub_id)
                for doi in doi_list:
                    result.append(processor.createPublicationObjectGraph(doi))

        return result 

    
    def getPublicationAuthors(self, publicationId):
        #Returns list[Person]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                df_a = processor.getPublicationAuthors(publicationId) 
                aut_group = ""
                for idx, row in df_a.iterrows():
                    aut_group = row["AutGroupInternalId"]
                    break
                authors = processor.getAuthorSetByAuthorGroupInternalId(aut_group)
                for author in authors:
                    result.append(author)
           

            else: #if type(processor) == TriplestoreQueryProcessor
                df_a = processor.getPublicationAuthors(publicationId)
                result.extend([(Person(row.givenName, row.familyName, {row.publication_id}))
                              for index, row in df_a.iterrows()])   

        return result

          
    def getPublicationsByAuthorName(self, authorPartialName):
        #Returns list[Publication]
        result = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                pubs_df = processor.getPublicationsByAuthorName(authorPartialName)
                doi_list = CreateListFromDataFrameColumn(pubs_df, "id")
                for doi in doi_list:
                    result.append(processor.createPublicationObjectFromDoiRelational(doi))


            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                doi_list = []
                pubs_df = processor.getPublicationsByAuthorName(
                    authorPartialName)
                for idx, row in pubs_df.iterrows():
                    pub_id = row["publication_id"]
                    doi_list.append(pub_id)

                for doi in doi_list:
                    result.append(processor.createPublicationObjectGraph(doi))    

        return result 


    def getDistinctPublishersOfPublications(self, pubIdList):
        #Returns list[Organization]
        result_prelim = []
        for processor in self.queryProcessor:
            if type(processor) == RelationalQueryProcessor:
                publishers_df = processor.getDistinctPublishersOfPublications(pubIdList)
                publisher_id_list_prelim = CreateListFromDataFrameColumn(publishers_df, "PublisherId") #Get this list to drop duplicates
                publisher_id_list = []
                publisher_id = ""
                for publisher in publisher_id_list_prelim:
                    if publisher not in publisher_id_list:
                        publisher_id_list.append(publisher)
                
                for publisher in publisher_id_list:
                    result_prelim.append(processor.getOrganizationObjectFromPublisherId(publisher))
                
            else: #i.e. if type(processor) == TriplestoreQueryProcessor
                publishers_df = processor.getDistinctPublishersOfPublications(
                    pubIdList)
                result_prelim.extend([(Organization(
                    {row.publication_id}, row.publisher_name)) for index, row in publishers_df.iterrows()])   

        result = [] #Elimiate duplicates
        for publisher in result_prelim:
            if publisher not in result:
                result.append(publisher)
        return result 

