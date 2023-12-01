from sparkplug_node_app import env
import json

node_topics = {
    'NBIRTH': f'spBv1.0/{env.SPARKPLUG_GROUP_ID}/NBIRTH/{env.SPARKPLUG_EDGE_NODE_ID}',
    'NDEATH': f'spBv1.0/{env.SPARKPLUG_GROUP_ID}/NBIRTH/{env.SPARKPLUG_EDGE_NODE_ID}',
    'NDATA': f'spBv1.0/{env.SPARKPLUG_GROUP_ID}/NDATA/{env.SPARKPLUG_EDGE_NODE_ID}',
    'NCMD': f'spBv1.0/{env.SPARKPLUG_GROUP_ID}/NCMD/{env.SPARKPLUG_EDGE_NODE_ID}'
}

node_config = {
    
}

node_state = {
    'NBIRTH-mid': None
}

metrics = [

]

