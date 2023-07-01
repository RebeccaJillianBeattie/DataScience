from impl import *

#Create relational database
rel_path = "relationalDatabase.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("Data/clean_relational_publications.csv")
rel_dp.uploadData("Data/clean_json_relational_data.json")

#Create RDF triplestore
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql" 
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("Data/graph_publications.csv")
grp_dp.uploadData("Data/graph_other_data.json")

#Create query processors
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)
grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)

#Create a generic query processor
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)

#Testing the methods
result_q1 = generic.getPublicationsPublishedInYear(2015)
result_q2_r = generic.getPublicationsByAuthorId("0000-0001-9857-1511") #relational
result_q2_g = generic.getPublicationsByAuthorId("0000-0003-4808-850X") #graph
result_q3 = generic.getMostCitedPublication()
result_q4 = generic.getMostCitedVenue()
result_q5_r = generic.getVenuesByPublisherId("crossref:239") #relational
result_q5_g = generic.getVenuesByPublisherId("crossref:237") #graph
result_q6_r = generic.getPublicationInVenue("issn:1588-2861") #relational
result_q6_g = generic.getPublicationInVenue("issn:1940-1493") #graph
result_q7_r = generic.getJournalArticlesInIssue("8", "26", "issn:0969-9988") #relational
result_q7_g = generic.getJournalArticlesInIssue("67", "6", "issn:2475-9066") #graph
result_q8_r = generic.getJournalArticlesInVolume("205", "issn:0144-8617") #relational
result_q8_g = generic.getJournalArticlesInVolume("235", "issn:0950-7051") #graph
result_q9_r = generic.getJournalArticlesInJournal("issn:1303-2917") #relational
result_q9_g = generic.getJournalArticlesInJournal("issn:2306-5729") #graph
result_q10 = generic.getProceedingsByEvent("IPMU") 
result_q11_r = generic.getPublicationAuthors("doi:10.3390/agronomy11081557") #relational 
result_q11_g = generic.getPublicationAuthors("doi:10.1111/tgis.12602") #graph
result_q12 = generic.getPublicationsByAuthorName("Ed") 
result_q13 = generic.getDistinctPublishersOfPublications(["doi:10.3390/educsci11040184", "doi:10.1007/978-3-030-62466-8_41", "doi:10.1371/journal.pone.0200929", "doi:10.1007/s10639-021-10720-y", "doi:10.1038/nprot.2015.124", "doi:10.1007/s11192-017-2436-5"])

print(result_q1)

