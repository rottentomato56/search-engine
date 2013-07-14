from __future__ import division
from operator import itemgetter
import pymongo
import numpy as np



# start with document vectors

#doc: [(term, weight), (term,weight)]

# a doc_vector should be of the dictionary form:
#
# v = {'doc_name': 'sample.txt', 'path': 'C:/Users/Bob/My Documents/sample.txt', 'term_weights': ['term1': weight1, 'term2': weight2, ...]}

		
class Index(object):
	def __init__(self):
	
		"""
			each record in the inverted index should be of the form:
				term : [(doc_id, length, term_weight)]
		"""
		client = pymongo.MongoClient()
		db = client.search_index
		self._document_vectors = db.doc_vectors # this collection should map all doc_id's to filenames and paths, along with the weights
		self._inverted_index = db.inverted_index # this collection contains terms and postings
		
		

	def _add_docs(self, doc_set):
	
		""" doc_set should be a list of dictionary objects """
		doc_list = []
		id_list = []
		for doc_vector in doc_set:
			#doc_id = doc_vector['doc_id']			we will use MongoDb's ObjectId
			doc_vector['norm'] = self.doc_norm(doc_vector)
			doc_list.append(doc_vector)
			
			if len(doc_list) == 10000:
				id_list += self._document_vectors.insert(doc_list)
				doc_list = []
		id_list += self._document_vectors.insert(doc_list)
		return id_list
			
	def _add_to_index(self, doc_vector, doc_id):
		doc_norm = doc_vector['norm']
		for term, weight in doc_vector['term_weights'].iteritems():
			entry = self._inverted_index.find_one({'term' : term})
			if entry:
				entry['postings'].append([doc_id, doc_norm, weight]) 
			else:
				entry = {'term': term, 'postings': [[doc_id, doc_norm, weight]]}
			print entry
			self._inverted_index.save(entry)
				
				
	def index(self, doc_set):
		""" calls the add method to add every single document vector in the doc_set to the index"""
		id_list = self._add_docs(doc_set)
		for i in range(len(id_list)):
			self._add_to_index(doc_set[i], id_list[i])
			
	def doc_norm(self, doc_vector):
		norm = 0
		for term_weight in doc_vector['term_weights'].itervalues():
			norm = norm + term_weight ** 2
		return np.sqrt(norm)
			
			
class SearchEngine(object):
	""" SearchEngine implements a linear vector space model with Euclidean norms"""
	
	# consider adding Boolean model/functionality in the future
	def __init__(self):
		self._index = Index()
		
	def search(self, query_vector, limit):
		""" query_vector must be a dictionary of weights for each term"""
		
		qnorm = self.query_norm(query_vector)
		
		result_scores = {}
		for term, weight in query_vector.iteritems():
			entry = self._index._inverted_index.find_one({'term' : term})
			if entry:
				for posting in entry['postings']:
					result_scores[posting[0]] = result_scores.get(posting[0], 0) + (posting[2] * weight / (qnorm * posting[1]))
		
		
			# implement own sorting algorithm later
		results = [(doc_id, score) for doc_id, score in result_scores.items()]
		results.sort(key=itemgetter(1))
		
		return results
		returned_results = []
		
		while len(returned_results) <= limit and results:
			posting = results.pop()
			doc_vector = self._index._document_vectors.find_one({'_id' : posting[0]})
			returned_results.append((doc_vector['name'], doc_vector['location']))
		return returned_results
				
	def cosine_similarity(self, query_vector, doc_vector):
		return 
	
	def query_norm(self, query_vector):
		return np.sqrt(sum(x**2 for x in query_vector.values()))