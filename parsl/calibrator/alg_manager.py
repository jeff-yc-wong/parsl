from itertools import product
from collections.abc import Iterable
from copy import deepcopy
from typing import Generator

class SchedulingAlg(object):
	def __init__(self, task_selection_scheme : str, worker_selection_scheme : str,num_cores_selection_scheme : str):
		self.task_selection_scheme=task_selection_scheme
		self.worker_selection_scheme=worker_selection_scheme
		self.num_cores_selection_scheme=num_cores_selection_scheme
		
	def modifyJSON(self,json):
		return _modifyJSON(json,self)
		
	def asDict(self):
		return{
			"task_selection_scheme":self.task_selection_scheme,
			"worker_selection_scheme":self.worker_selection_scheme,
			"num_cores_selection_scheme":self.num_cores_selection_scheme
		}	
		
	def asTuple(self):
		return self.task_selection_scheme,self.worker_selection_scheme,self.num_cores_selection_scheme
		
	def __repr__(self):
		return str(self.asDict())
		
class AlgManager (object):
	def __init__(self, task_selection_schemes: Iterable[str], worker_selection_schemes: Iterable[str],num_cores_selection_schemes: Iterable[str]):
		self.task_selection_schemes=task_selection_schemes
		self.worker_selection_schemes=worker_selection_schemes
		self.num_cores_selection_schemes=num_cores_selection_schemes

	def asDict(self) -> Generator[dict[str,str], None, None] :
		for alg in self.asTuple():
			yield {
				"task_selection_scheme":alg[0],
				"worker_selection_scheme":alg[1],
				"num_cores_selection_scheme":alg[2]
			}
	def asObj(self) -> Generator[SchedulingAlg, None, None]:
		for alg in self.asTuple():
			yield SchedulingAlg(alg[0],alg[1],alg[2])
			
	def asTuple(self) -> Generator[tuple[str,str,str], None, None]: 
		for alg in product(self.task_selection_schemes,self.worker_selection_schemes,self.num_cores_selection_schemes):
			yield alg
			
	def modifyJSON(self,json,args : dict[str,str] | SchedulingAlg | tuple[str,str,str]):
		return _modifyJSON(json,args)

def _modifyJSON(json,args : dict[str,str] | SchedulingAlg | tuple[str,str,str]):
	json=deepcopy(json)
	argTuple=args
	if isinstance(args, dict):
		argTuple=(args["task_selection_scheme"],args["worker_selection_scheme"],args["num_cores_selection_scheme"])
	elif isinstance(args, SchedulingAlg):
		argTuple = args.asTuple()
	json["scheduling"]["task_selection_scheme"]=argTuple[0]
	json["scheduling"]["worker_selection_scheme"]=argTuple[1]
	json["scheduling"]["num_cores_selection_scheme"]=argTuple[2]
	return json
	

		
if __name__=="__main__":
	print("Testing Alg Manager")
	test=AlgManager(("task_scheme1","task_scheme2","task_scheme3"),{"worker_scheme1","worker_scheme2","worker_scheme3","worker_scheme4"},["core_scheme1","core_scheme2"])
	testJson={
		"otherkey":{},
		"scheduling": {
			"task_selection_scheme": "most_flops",
			"worker_selection_scheme": "most_idle_cores",
			"num_cores_selection_scheme": "one_core",
			"task_scheduling_overhead": 1
		}
	}

	for alg in test.asDict():
		print(test.modifyJSON(testJson,alg))
		print(alg)
		
	for alg in test.asObj():
		print(test.modifyJSON(testJson,alg))
		print(alg.modifyJSON(testJson))
		print(alg)
		
	for alg in test.asTuple():
		print(test.modifyJSON(testJson,alg))
		print(alg)
	