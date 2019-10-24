"""
This file is part of TriaClick Associative Semiotic Hypergraph Engine
(C) 2018-2019 Athanassios I. Hatzis
Licensed under the TriaClick Open Source License Agreement (TOSLA)
You may not use this file except in compliance with TOSLA.
The files subject to TOSLA are grouped in this directory to clearly separate them from files
in the parent directory that are licensed under GNU Affero General Public License v.3.0.
You should retain this header in the file and a copy of the LICENSE_TOSLA file in the current directory
"""
import matplotlib.pyplot as plt
import networkx as nx
from pydot import Graph,Cluster,Node,Edge
from graphviz import Source as GVSource
class HGPyDot(Graph):
 def __init__(self,**kwargs):
  super().__init__(**kwargs)
 @classmethod
 def pydot_bipartite(cls,gseperation=0.2,nseperation=0.6,left_label='Left Set',right_label='Right Set',left_ncolor='green',right_ncolor='brown1',left_nodes=None,right_nodes=None,left_nlabels=None,right_nlabels=None):
  pydot_graph=cls(graph_type='digraph')
  pydot_graph.set_layout('dot')
  pydot_graph.set_splines('false')
  pydot_graph.set_rankdir('TB')
  pydot_graph.set_ranksep(gseperation)
  pydot_graph.set_nodesep(nseperation)
  cluster_left=cls.bipartite_cluster('Left',left_label,left_nodes,left_nlabels,left_ncolor)
  pydot_graph.add_subgraph(cluster_left)
  cluster_right=cls.bipartite_cluster('Right',right_label,right_nodes,right_nlabels,right_ncolor)
  pydot_graph.add_subgraph(cluster_right)
  return pydot_graph
 @classmethod
 def bipartite_cluster(cls,cluster_name,cluster_label,cluster_nodes,cluster_nlabels,node_color):
  cluster=Cluster(cluster_name)
  cluster.set_label(cluster_label)
  cluster.set_fillcolor('lightgrey')
  cluster.set_style('filled')
  cluster.set_rank('same')
  for n,label in zip(cluster_nodes,cluster_nlabels):
   cluster.add_node(cls.bipartite_node(n,label,node_color))
  for first,second in zip(cluster_nodes,cluster_nodes[1:]):
   cluster.add_edge(cls.bipartite_edge(first,second))
  return cluster
 @classmethod
 def bipartite_node(cls,node_name,node_label,node_color):
  node=Node(node_name,label=node_label,fillcolor=node_color,style='filled',penwidth=2,width=0.3,shape='oval',fontsize=11)
  return node
 @classmethod
 def bipartite_edge(cls,begin,end):
  edge=Edge(begin,end)
  return edge
 def bipartite_mapping(self,pairs):
  self.set_edge_defaults(constraint='false')
  for left,right in pairs:
   self.add_edge(self.bipartite_edge(left,right))
  return self
 def draw(self,output_format=None,output_dir=None,output_filename=None):
  return GVSource(self.to_string(),format=output_format,directory=output_dir,filename=output_filename)
class HGraph(nx.Graph):
 def __init__(self,**kwargs):
  super().__init__(**kwargs)
  self.npositions=None
 @staticmethod
 def _set_figure(width=16,height=10,left=0.01,right=0.99,top=0.99,bottom=0.01,title=None,axis='off'):
  plt.clf()
  wh=(width,height)
  plt.rcParams['figure.figsize']=wh
  plt.subplots_adjust(left=left,right=right,top=top,bottom=bottom)
  if title is not None:
   plt.title(title,size=15)
  if axis=='off':
   plt.axis('off')
  elif axis=='on':
   plt.axis('on')
 def _get_labels(self,attrib,objects):
  if objects=='nodes':
   return nx.get_node_attributes(self,attrib)
  elif objects=='edges':
   return nx.get_edge_attributes(self,attrib)
 def _set_node_positions(self,layout='shell'):
  if layout=='spring':
   self.npositions=nx.spring_layout(self,k=0.75,iterations=500,scale=10)
  elif layout=='shell':
   self.npositions=nx.shell_layout(self)
  elif layout=='circular':
   self.npositions=nx.circular_layout(self)
  elif layout=='graphviz':
   self.npositions=nx.nx_pydot.graphviz_layout(self,prog='neato')
  elif layout=='pydot':
   self.npositions=nx.nx_pydot.pydot_layout(self,prog='dot')
 def _draw_edges(self,width=2,color='g',filter_list=None):
  if filter_list is None:
   filter_list=self.edges
  nx.draw_networkx_edges(self,self.npositions,width=width,edge_color=color,edgelist=filter_list)
 def _draw_edge_labels(self,placement=0.75,size=18,weight='normal',attribute='label'):
  if attribute is None:
   elabels=None
  else:
   elabels=self._get_labels(attribute,'edges')
  nx.draw_networkx_edge_labels(self,self.npositions,edge_labels=elabels,font_size=size,font_weight=weight,label_pos=placement)
 def _draw_nodes(self,size=2500,shape='o',color='c',filter_list=None):
  if filter_list is None:
   filter_list=self.nodes
  nx.draw_networkx_nodes(self,self.npositions,nodelist=filter_list,node_size=size,node_shape=shape,node_color=color)
 def _draw_node_labels(self,attribute='key',size=18,weight='normal'):
  if attribute is None:
   nlabels=None
  else:
   nlabels=self._get_labels(attribute,'nodes')
  nx.draw_networkx_labels(self,self.npositions,labels=nlabels,font_size=size,font_weight=weight)
 def draw(self,graph_layout='spring',graph_width=10,graph_height=8,graph_title=None,graph_axis='off',node_size=2000,node_shape='o',node_color='yellow',nlabels_size=11,nlabels_weight='bold',nattribute='label',elabels_size=11,elabels_weight='bold',eattribute='label',eplacement=0.55,ewidth=2,ecolor='darkred'):
  self._set_figure(width=graph_width,height=graph_height,title=graph_title,axis=graph_axis)
  self._set_node_positions(layout=graph_layout)
  self._draw_nodes(size=node_size,shape=node_shape,color=node_color,filter_list=self.nodes)
  self._draw_node_labels(size=nlabels_size,weight=nlabels_weight,attribute=nattribute)
  self._draw_edge_labels(size=elabels_size,weight=elabels_weight,placement=eplacement,attribute=eattribute)
  self._draw_edges(width=ewidth,color=ecolor,filter_list=self.edges)
  plt.show()
class ASERD(HGraph):
 def __init__(self,**kwargs):
  super().__init__(**kwargs)
  self.npositions=None
 def __repr__(self):
  result=f'ASERD{self.key}:{self.alias}'
  return result
 @property
 def name(self):
  return self.graph['name']
 @property
 def alias(self):
  return self.graph['alias']
 @property
 def key(self):
  return self.graph['key']
 def get_bfs_edges(self,start_node):
  return[(self.nodes[edge[0]]['key'],self.nodes[edge[1]]['key'],self.edges[edge]['key'])for edge in nx.bfs_edges(self,start_node)]
 def add_asets(self,asets):
  edge_labels=[]
  for key,head_aset in asets.items():
   jattributes=set(head_aset.ent.get_attributes(out='objects',junction=True))
   for junction_attr in jattributes:
    label=junction_attr.alias
    if label not in edge_labels:
     edge_labels.append(label)
     k1,k2=junction_attr.key[1],junction_attr.key[2]
     ju_parents_dict={obj.nID:obj for obj in junction_attr.entities.all()}
     tail_ent=[v for k,v in ju_parents_dict.items()if k!=head_aset.ent.node.nID][0]
     tail_aset=asets[tail_ent.dim3,tail_ent.dim2]
     self.add_edge(head_aset.key[1],tail_aset.key[1],label=label,key=(k1,k2))
  for node in self.nodes:
   aset=asets[self.key[0],node]
   self.nodes[node]['label']=aset.alias
   self.nodes[node]['key']=(self.key[0],node)
  return edge_labels
 def info(self):
  return nx.info(self)


