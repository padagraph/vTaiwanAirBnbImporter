import json
from collections import namedtuple

from botapi import Botagraph, BotApiError
from reliure.types import Text

NodeType = namedtuple("NodeType", "name description properties")
EdgeType = namedtuple("EdgeType", "name description properties")

PDG_HOST = "http://g0v-tw.padagraph.io"
PDG_KEY = ""
GRAPHNAME = "vTaiwan x AirBnB"
DESCRIPTION = "Opinion Graph about AirBnb in Taiwan"
TAGS = ["vTaiwan", "airBnB", "pol.is"]

N_COMMENTS = 227

NodeAgreedComment = NodeType("AgreedComment", "a comment participants agreed with",  {"id": Text(),
                                        "body": Text(),
                                        "label": Text(),
                                        "shape": Text()})

NodeDisagreedComment = NodeType("DisagreedComment", "a comment participants disagreed with",  {"id": Text(),
                                        "body": Text(),
                                        "label": Text(),
                                        "shape": Text()})

NodeUser = NodeType("Participant", "",  {"id": Text(),
                                        "label": Text(),
                                        "shape": Text()})

EdgeVote = EdgeType("vote", "",{})
EdgeOppositeComment = EdgeType("opposite", "",{})
EdgeAuthored = EdgeType("authored", "",{})


comments = json.load(open("./data/airbnb-2015-10-02-comments.csv.json"))
votes = json.load(open("./data/airbnb-2015-10-02-participants-votes.csv.json"))

bot = Botagraph(PDG_HOST, PDG_KEY)
bot.create_graph(GRAPHNAME, {'description': DESCRIPTION, "tags": TAGS})


# Posting Nodes and Edges Types

nodetypes_uuids = {}
edgetypes_uuids = {}
for nt in [NodeAgreedComment, NodeDisagreedComment, NodeUser]:
    nodetypes_uuids[nt.name] = bot.post_nodetype(GRAPHNAME, nt.name, nt.description, nt.properties)

for et in [EdgeVote, EdgeAuthored, EdgeOppositeComment]:
    edgetypes_uuids[et.name] = bot.post_edgetype(GRAPHNAME, et.name, et.description, et.properties)



def getCommentNodesIterator():
    for row in comments['data']:
        d = {k:v for k,v in zip(comments['select_columns'],row)}
        # agree
        props = {'id': d['comment-id'],
                 'body':d['comment-body'],
                 'polarity': '+',
                 'label':"+" + d['comment-body'][:5] + ("" if len(d['comment-body']) < 6 else "..."),
                 'shape':'square'}
        yield { 'nodetype': nodetypes_uuids[NodeAgreedComment.name],
                'properties': props}
        # disagree
        props = {'id': d['comment-id'],
                 'body':d['comment-body'],
                 'polarity': '-',
                 'label':"-" + d['comment-body'][:5] + ("" if len(d['comment-body']) < 6 else "..."),
                 'shape':'triangle'}
        yield { 'nodetype': nodetypes_uuids[NodeDisagreedComment.name],
                'properties': props}


def getUserNodesIterator():
    for row in votes['data']:
        d = {k:v for k,v in zip(votes['select_columns'],row)}
        props = {'id': d['participant'],
                 'label': d['participant'],
                 'shape': 'circle'}
        yield {'nodetype': nodetypes_uuids[NodeUser.name],
                'properties': props}


# Posting Nodes
nodes_uuids = {}
for node, uuid in bot.post_nodes(GRAPHNAME, getCommentNodesIterator()):
    nid = node['properties']['polarity'] + node['properties']['id']
    nodes_uuids[nid] = uuid


for node, uuid in bot.post_nodes(GRAPHNAME, getUserNodesIterator()):
    nid = node['properties']['id']
    nodes_uuids[nid] = uuid


# Posting Edges

def getVoteIterator():
    for row in votes['data']:
        vote_dict = {k:v for k,v in zip(votes['select_columns'],row)}
        user_id = nodes_uuids[vote_dict['participant']]
        for i in range(N_COMMENTS):
            if vote_dict[str(i)] == '-1':
                com_id = nodes_uuids['-' + str(i)]
                yield {'edgetype': edgetypes_uuids[EdgeVote.name],
                        'source': user_id,
                        'target': com_id,
                        'properties': {}}
            if vote_dict[str(i)] == '1':
                com_id = nodes_uuids['+' + str(i)]
                yield {'edgetype': edgetypes_uuids[EdgeVote.name],
                        'source': user_id,
                        'target': com_id,
                        'properties': {}}


def getOppositeIterator():
    for row in comments['data']:
        d = {k:v for k,v in zip(comments['select_columns'],row)}
        c1 = '+' + d['comment-id']
        c2 = '-' + d['comment-id']
        yield {'edgetype': edgetypes_uuids[EdgeOppositeComment.name],
                'source': c1,
                'target': c2,
                'properties': {}}

list(bot.post_edges(GRAPHNAME, getVoteIterator()))
list(bot.post_edges(GRAPHNAME, getOppositeIterator()))



