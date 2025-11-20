from fastapi import FastAPI, Query, Depends, HTTPException, status
from SPARQLWrapper import SPARQLWrapper, CSV, JSON
from fastapi.middleware.cors import CORSMiddleware
import json

from fastapi.security import OAuth2AuthorizationCodeBearer
from jose import jwt, JWTError
import httpx


app = FastAPI(root_path="/api/")

origins = [
    "http://localhost",  # Add specific origins if you don't want to allow all
    "http://localhost:8080",
    "http://127.0.0.1",
    "http://127.0.0.1:8080",
    "*", # Allow all origins (use with caution in production)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

#sparql = SPARQLWrapper("https://semantics.istc.cnr.it/hacid/sparql")
sparql = SPARQLWrapper("http://w3id.org/hacid/cs/sparql")
#sparql.setCredentials("hacid", "hacid")

# Returns all classes in the knowledge graph
@app.get("/knowledge-graph/classes")
def find_classes():
    query = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?class ?classLabel WHERE
    {
        ?class rdf:type owl:Class .
        ?class rdfs:label ?classLabel. 
        FILTER(?class != owl:Thing && ?class != owl:Nothing && !isBlank(?class)).
    }
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    return results['results']['bindings']

# Returns instances of a given class in the knowledge graph
# param class_uri: The URI of the class to find instances for
# param startswith: Optional filter to return only instances whose labels start with this string
# param contains: Optional filter to return only instances whose labels contain these comma-separated substrings

@app.get("/knowledge-graph/instances")
def find_class_instances(class_uri: str, startswith: str|None = None, contains: str | None = None):
    startswith_filter = ""
    contains_filter = ""

    if(startswith!=None):
        startswith_filter = f"FILTER(STRSTARTS(LCASE(?classInstanceLabel), LCASE('{startswith}')))." if startswith else ""

    if(contains!=None):
        contains_filter = "\n".join([f"FILTER CONTAINS(LCASE(str(?classInstanceLabel)), LCASE('{c}'))." for c in (contains.split(',') or [])])

    query = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?classInstance ?classInstanceLabel WHERE {{
        ?classInstance a <{class_uri}> .
        ?classInstance rdfs:label ?classInstanceLabel .
        {startswith_filter}
        {contains_filter}
        FILTER(lang(?classInstanceLabel) = "en-gb" || lang(?classInstanceLabel) = "en-us" || lang(?classInstanceLabel) = "en")
    }} LIMIT 1000
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    return results['results']['bindings']


# Returns role instances of a given class in the knowledge graph
# param class_uri: The URI of the class to find instances for
# param startswith: Optional filter to return only instances whose labels start with this string
# param contains: Optional filter to return only instances whose labels contain these comma-separated substrings
# NOTE: thid method is no longer used. Now roles are retrieved via the /knowledge-graph/task-data endpoint

@app.get("/knowledge-graph/roles")
def find_roles(class_uri: str, startswith: str|None = None, contains: str | None = None ):
    startswith_filter = ""
    contains_filter = ""

    if(startswith!=None):
        startswith_filter = f"FILTER(STRSTARTS(LCASE(?classInstanceLabel), LCASE('{startswith}')))." if startswith else ""

    if(contains!=None):
        contains_filter = "\n".join([f"FILTER CONTAINS(LCASE(str(?classInstanceLabel)), LCASE('{c}'))." for c in (contains.split(',') or [])])

    query = f"""
BASE <https://w3id.org/hacid/data/cs/wf/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX top: <https://w3id.org/hacid/onto/top-level/>
PREFIX method_role: <app-profile/roles/Method>
PREFIX ops: <ops/>

SELECT DISTINCT
    ?classInstance ?classInstanceLabel
WHERE {{
    {{
        <{class_uri}> top:hasExpectedType  ?range_class.
        ?classInstance a ?range_class ;
              rdfs:label ?classInstanceLabel .
    }} UNION {{
        FILTER (<{class_uri}> = <https://w3id.org/hacid/data/cs/wf/app-profile/roles/Method>)
        {{
            ?op top:isRealizedByMethod ?classInstance .
        }} UNION {{
			?op top:isSpecializedBy ?classInstance.
			FILTER(?op != ops:FilterClimateProjections)
        }} UNION {{
			?op top:isRealizedByPlan/top:definesTask/top:executedThroughOperation ?classInstance.
			FILTER NOT EXISTS {{?other_op1 top:isRealizedByMethod|top:isSpecializedBy ?classInstance}}
			FILTER NOT EXISTS {{?classInstance top:isRealizedByMethod|top:isSpecializedBy|(top:isRealizedByPlan/top:definesTask/top:executedThroughOperation) ?other_op2}}
			FILTER (?op != ops:ClimateDataCollection)
			FILTER (?op != ops:ClimateProjectionSelection)
			FILTER (?op != ops:ClimateServiceCaseResolution)
        }}
		?op rdfs:label ?op_label.
		?classInstance rdfs:label ?method_label.
		BIND(CONCAT(?op_label, ' - ', ?method_label) AS ?classInstanceLabel)
    }}
    
    {startswith_filter}
    {contains_filter}
 
    }}ORDER BY ?classInstanceLabel LIMIT 1000
"""
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    return results['results']['bindings']

# Returns role instances of a given entity type in the knowledge graph
# param entity_type: Generic parameter representing the entity type to find role instances for
# param startswith: Optional filter to return only instances whose labels start with this string
# param contains: Optional filter to return only instances whose labels contain these comma-separated substrings
@app.get("/knowledge-graph/task-data")
def find_roles(entity_type: str, startswith: str|None = None, contains: str | None = None ):

    entity_type = "https://w3id.org/hacid/data/cs/wf/ops/FilterClimateProjectionsByVariable/roles/SelectedVariable" # for testing purposes

    startswith_filter = ""
    contains_filter = ""

    if(startswith!=None):
        startswith_filter = f"FILTER(STRSTARTS(LCASE(?classInstanceLabel), LCASE('{startswith}')))." if startswith else ""

    if(contains!=None):
        contains_filter = "\n".join([f"FILTER CONTAINS(LCASE(str(?classInstanceLabel)), LCASE('{c}'))." for c in (contains.split(',') or [])])

    query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX top: <https://w3id.org/hacid/onto/top-level/> 

        SELECT 
            ?classInstance ?classInstanceLabel
        WHERE {{
            <{entity_type}>
                top:hasExpectedType/(
                    (owl:unionOf|owl:intersectionOf)/rdf:rest*/rdf:first |
                    owl:allValuesFrom
                )* ?itemClass.
            FILTER(?itemClass NOT IN (top:Interval)).
            {{
                ?classInstance a ?itemClass.
            }} UNION {{
                ?itemClass [
                    a owl:Restriction;
                    owl:onProperty ?p;
                    owl:hasValue ?o
                ].
                ?classInstance ?p ?o.
            }}.
            ?classInstance rdfs:label ?classInstanceLabel
            {startswith_filter}
            {contains_filter}
        }}
        ORDER BY ?classInstanceLabel
        LIMIT 1000
    """

#     query = f"""
# BASE <https://w3id.org/hacid/data/cs/wf/>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX top: <https://w3id.org/hacid/onto/top-level/>
# PREFIX method_role: <app-profile/roles/Method>
# PREFIX ops: <ops/>

# SELECT DISTINCT
#     ?classInstance ?classInstanceLabel
# WHERE {{
#     {{
#         <{entity_type}> top:hasExpectedType  ?range_class.
#         ?classInstance a ?range_class ;
#               rdfs:label ?classInstanceLabel .
#     }} UNION {{
#         FILTER (<{entity_type}> = <https://w3id.org/hacid/data/cs/wf/app-profile/roles/Method>)
#         {{
#             ?op top:isRealizedByMethod ?classInstance .
#         }} UNION {{
# 			?op top:isSpecializedBy ?classInstance.
# 			FILTER(?op != ops:FilterClimateProjections)
#         }} UNION {{
# 			?op top:isRealizedByPlan/top:definesTask/top:executedThroughOperation ?classInstance.
# 			FILTER NOT EXISTS {{?other_op1 top:isRealizedByMethod|top:isSpecializedBy ?classInstance}}
# 			FILTER NOT EXISTS {{?classInstance top:isRealizedByMethod|top:isSpecializedBy|(top:isRealizedByPlan/top:definesTask/top:executedThroughOperation) ?other_op2}}
# 			FILTER (?op != ops:ClimateDataCollection)
# 			FILTER (?op != ops:ClimateProjectionSelection)
# 			FILTER (?op != ops:ClimateServiceCaseResolution)
#         }}
# 		?op rdfs:label ?op_label.
# 		?classInstance rdfs:label ?method_label.
# 		BIND(CONCAT(?op_label, ' - ', ?method_label) AS ?classInstanceLabel)
#     }}
    
#     {startswith_filter}
#     {contains_filter}
 
#     }}ORDER BY ?classInstanceLabel LIMIT 1000
# """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    return results['results']['bindings']


# Returns all tasks in the knowledge graph
# Currently reads from a static JSON file. To be replaced with SPARQL query in the future.
@app.get("/knowledge-graph/tasks")
def get_tasks():
    with open("data/tasks.json", "r") as f:
        data = json.load(f)
    return data


#     query = """
# BASE <https://w3id.org/hacid/data/cs/wf/>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX top: <https://w3id.org/hacid/onto/top-level/>
# PREFIX wf: <https://w3id.org/hacid/data/cs/wf/>
# PREFIX info_prep_task: <plans/ClimateInformationStudyPlan/tasks/ClimateInformationPreparation>
# PREFIX data_sel_task: <plans/ClimateDataCollectionPlan/tasks/BaseClimateProjectionSelection>
# PREFIX method_role: <app-profile/roles/Method>
# PREFIX preproc: <app-profile/ops/Preprocessing>
# PREFIX obs_analysis: <app-profile/ops/ObservationAnalysis>
# PREFIX ds_select: <app-profile/ops/DatasetSelection>

# # Get tasks

# SELECT 
# 	?task_label ?op ?sub_op ?sub_op_label ?role ?role_label
# 	(COUNT(DISTINCT ?following_task) AS ?num_following_tasks)
# 	(COUNT(DISTINCT ?following_sub_task) AS ?num_following_sub_tasks)
# WHERE {
#     {
#         {
#             <plans/ClimateInformationStudyPlan> top:definesTask ?task.
#       		?op top:isRealizedByPlan/top:definesTask ?sub_task.
#             FILTER (?sub_task != data_sel_task:)
#             VALUES ?main_order {1}
#         } UNION {
#             VALUES (?task ?main_order) {
#                 (info_prep_task: 0)
#             }
#     		data_sel_task: top:executedThroughOperation/top:isRealizedByPlan/top:definesTask ?sub_task.
#         }
#         ?sub_task top:directlyPrecedes* ?following_sub_task;
#             top:executedThroughOperation ?sub_op.
#         ?sub_op rdfs:label ?sub_op_label.
#         OPTIONAL {
#             ?sub_op top:hasInputRole ?role.
#             ?role rdfs:label ?role_label
#         }
#         OPTIONAL {
#             ?sub_op top:isRealizedByMethod|top:isRealizedByPlan ?method.
#             VALUES (?role ?role_label) {(method_role: "Method"@en)}
#         }
#     } UNION {
#         VALUES (?task ?sub_op ?sub_op_label ?main_order ?role ?role_label) {
#             (info_prep_task: preproc: "Preprocessing"@en 1 method_role: "Method"@en)
#             (info_prep_task: obs_analysis: "Observation analysis"@en 1 method_role: "Method"@en)
#             (info_prep_task: ds_select: "Dataset selection"@en 0 UNDEF UNDEF)
#         }
#     }
# 	?task rdfs:label ?task_label;
#   		top:executedThroughOperation ?op;
#     	top:directlyPrecedes* ?following_task.
#     ?op rdfs:label ?op_label.
# }
# GROUP BY ?task_label ?op ?sub_op ?sub_op_label ?role ?role_label ?main_order
# ORDER BY ?main_order DESC(?num_following_tasks) DESC(?num_following_sub_tasks)
#     """

#     sparql.setQuery(query)
#     sparql.setReturnFormat(JSON)
#     results = sparql.query().convert()
#     # Extract the bindings from the results
#     bindings = results['results']['bindings']
    
#     # Create a dictionary to store tasks and their subtasks
#     task_dict = {}
    
#     # Process each binding
#     for binding in bindings:
#         # Extract task information
#         task_uri = binding['op']['value']
#         task_label = binding['task_label']['value']
        
#         # Extract subtask information
#         subtask_uri = binding['sub_op']['value']
#         subtask_label = binding['sub_op_label']['value']
        
#         # Get output role if exists
#         role_uri = binding.get('role', {}).get('value', None)
        
#         # Create task entry if it doesn't exist
#         if task_uri not in task_dict:
#             task_dict[task_uri] = {
#                 'task_label': task_label,
#                 'task_uri': task_uri,
#                 'children': []
#             }
        
#         # Create subtask entry
#         subtask_entry = {
#             'task_label': subtask_label,
#             'task_uri': subtask_uri,
#             'parent_task_label': task_label,
#             'parent_task_uri': task_uri
#         }
        
#         # Add output role if it exists
#         if role_uri:
#             subtask_entry['role_uri'] = role_uri
#             subtask_entry['range_uri'] = role_uri
#             subtask_entry['role_label'] = binding['role_label']['value']
        
#         # Add subtask to task's children
#         task_dict[task_uri]['children'].append(subtask_entry)
    
#     # Convert dictionary to list
#     result = list(task_dict.values())

#     return result
#    # return json.loads(tasks.strip())


# Returns all hazards in the knowledge graph
# param startswith: Optional filter to return only hazards whose labels start with this string
# param contains: Optional filter to return only hazards whose labels contain these comma-separated substrings
# NOTE: Mocked up query for demonstration purposes. To be replaced with actual query that returns hazard data in the knowledge graph.
@app.get("/knowledge-graph/hazards")
def find_hazards(startswith: str|None = None, contains: str | None = None):
    startswith_filter = ""
    contains_filter = ""

    if(startswith!=None):
        startswith_filter = f"FILTER(STRSTARTS(LCASE(?hazardLabel), LCASE('{startswith}')))." if startswith else ""

    if(contains!=None):
        contains_filter = "\n".join([f"FILTER CONTAINS(LCASE(str(?hazardLabel)), LCASE('{c}'))." for c in (contains.split(',') or [])])

    query = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?hazard ?hazardLabel WHERE {{
        ?hazard rdf:type owl:Class .
        ?hazard rdfs:label ?hazardLabel .
        {startswith_filter}
        {contains_filter}
        FILTER(lang(?hazardLabel) = "en-gb" || lang(?hazardLabel) = "en-us" || lang(?hazardLabel) = "en")
    }} LIMIT 1000
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    return results['results']['bindings']

@app.get("/knowledge-graph/sparql")
def find_class_instances(query: str):
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    return results
