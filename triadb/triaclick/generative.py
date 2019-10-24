"""
This file is part of TriaClick Associative Semiotic Hypergraph Engine
(C) 2018-2019 Athanassios I. Hatzis
Licensed under the TriaClick Open Source License Agreement (TOSLA)
You may not use this file except in compliance with TOSLA.
The files subject to TOSLA are grouped in this directory to clearly separate them from files
in the parent directory that are licensed under GNU Affero General Public License v.3.0.
You should retain this header in the file and a copy of the LICENSE_TOSLA file in the current directory
"""
from functools import wraps
class GenerativeBase(object):
 def _generate(self):
  s=self.__class__.__new__(self.__class__)
  s.__dict__=self.__dict__.copy()
  return s
def _generative(func):
 @wraps(func)
 def decorator(self,*args,**kw):
  self=self._generate()
  func(self,*args,**kw)
  return self
 return decorator


