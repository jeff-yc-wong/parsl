#!/usr/bin/env python
import copy
import json
import sys
import simcal as sc
from typing import Any
from parsl.calibrator.alg_manager import *

# There are 3 different simulators for the 3 different situations we will run them in
# SchedulingSimulator is for making scheduling decisions.  It is given the state parts of the json (ie the "workflow" sub object) at each run and is initialized with a calibration and a set of scheduling algorithms, and a metric to use.  Run will then run the simulator on that state with that calibration for each algorithm and return the algorithm(s) with the lowest score according to the metric.  THIS VERSION IS NOT INTENDED TO BE RAN WITH SIMCAL

# CalibrationSimulator is for offline calibration of the simulator.  It is given a calibration at each run and is initialized with a loss function to use and a list of "ground truth" executions to use.  These include the workflow specs and the schedule used for each groundtruth run as well as the expected output.  Run will then run the simulator for each execution in the ground truth and return a scalar using the loss function

# ErrorCorrectionSimulator is for online error correction of the simulator.  It is given a calibration at each run and is initialized with a loss function to use and the state of a single partial execution, and a scheduling algorithm.  Run will then run the simulator using that state and return a scalar using the loss function
class Simulator(sc.Simulator):
	def __init__(self,simulator_path,json_template,verbosity = None):
		self.verbosity=[]
		if verbosity:
			self.verbosity=verbosity
		self.simulator_path=simulator_path
		self.json_template=json_template
	def modifyJSON(self,json_template,mod):
		if not isinstance(json_template, dict) or not isinstance(mod, dict):
			return copy.deepcopy(mod)  # Override A's value with B's if B is not a dict

		merged = copy.deepcopy(json_template)  # Create a copy of A to preserve immutability
		for key, value in mod.items():
			if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
				merged[key] = self.modifyJSON(merged[key], value)  # Recursive merge
			else:
				merged[key] = copy.deepcopy(value)  # Override with B's value

		return merged
	def exec(self,json_args,env):
		output = env.bash(self.simulator_path,("--json_input",json.dumps(json_args)))
		try:
			json_output=json.loads(output[0])
		except:
			if "parse_error" in self.verbosity:
				print(json_args,file=sys.stderr)
				print(output,file=sys.stderr)
			raise
		if len(output[1].split("\n"))>3 and "sim_error" in self.verbosity:
			print(json_args,file=sys.stderr)
			print(output,file=sys.stderr)
		return json.loads(output[0])

def _exec(simulator: Simulator, alg,state,stoptime):
	return alg, simulator.supercall({"state":state,"alg":alg},stoptime)
	
class SchedulingSimulator(Simulator):
	def __init__(self,simulator_path,json_template,calibration,algs,metric,coordinator,verbosity = None):
		super().__init__(simulator_path,json_template,verbosity)
		self.algs=algs
		self.metric=metric
		self.calibration=calibration
		self.coordinator=coordinator
		if coordinator is None:
			self.coordinator = sc.coordinators.Base()

	def run(self,env,args):
		alg=args["alg"]
		state=args["state"]
		json_args=alg.modifyJSON(self.json_template)
		json_args=self.modifyJSON(json_args,state)
		json_args=self.modifyJSON(json_args,self.calibration)
		output=self.exec(json_args,env)
		return self.metric(output)
	def supercall(self, args: Any, stoptime: int | float | None = None) -> str:
		return super().__call__(args,stoptime)
	def __call__(self, args: Any, stoptime: int | float | None = None) -> str:
		best=None
		bestScore=None
		for alg in self.algs.asObj():
			self.coordinator.allocate(_exec, (self, alg, args, stoptime))
			results = self.coordinator.collect()
			for current, score in results:
				if score is None:
					continue
				if bestScore is None or score < bestScore:
					best = [current]
					bestScore=score
				elif  score==bestScore:
					best.append(alg)
		results = self.coordinator.await_all()
		for current, score in results:
			if score is None:
				continue
			if bestScore is None or score < bestScore:
				best = [current]
				bestScore=score
			elif  score==bestScore:
				best.append(alg)
				
		return best
		
class CalibrationSimulator(Simulator):
	def __init__(self,simulator_path,json_template,verbosity = None):
		super().__init__(simulator_path,json_template,verbosity)
	
	def run(self,env,args):
		pass
		
		
class ErrorCorrectionSimulator(Simulator):
	def __init__(self,simulator_path,json_template,verbosity = None):
		super().__init__(simulator_path,json_template,verbosity)
	
	def run(self,env,args):
		pass
		
	
if __name__ == "__main__":
	class ParseException(Exception):
		pass
	import argparse
	import traceback
	import parsl.calibrator.metrics as metrics
	
	try:
		sub=sys.argv[1].lower()
		if "scheduling".startswith(sub) or sub == "schedule":
			sys.argv[0]+=" "+sys.argv[1]
			del sys.argv[1]
			parser = argparse.ArgumentParser(description="Simulation Argument Parser")
			parser.add_argument("-c", "--calibration", type=str, help="Calibration JSON as string", required=False)
			parser.add_argument("-s", "--state", type=str, help="State JSON as string", required=False)
			parser.add_argument("-p", "--simulator_path", type=str, help="Path to the simulator", required=True)
			parser.add_argument("-t", "--template", type=str, help="Template JSON as string", required=True)
			parser.add_argument("-m", "--metric", type=str, help="Metric string", required=True)
			parser.add_argument("-n", "--num_threads", type=int, help="Number of threads", required=False)
			parser.add_argument("-tss", "--task_selection_schemes", nargs='+', type=str, help="Task selection schemes", required=True)
			parser.add_argument("-wss", "--worker_selection_schemes", nargs='+', type=str, help="Worker selection schemes", required=True)
			parser.add_argument("-nss", "--num_cores_selection_schemes", nargs='+', type=str, help="Number of cores selection schemes", required=True)
			parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose mode", required=False)
			try:
				args=parser.parse_args()
				if(args.calibration):
					calibration=json.loads(args.calibration)
				else:
					calibration={}
				if(args.state is not None):
					state=json.loads(args.state)
				else:
					state={}	
				try: 
					with open(args.template) as template_file:
							template=json.load(template_file)
				except FileNotFoundError:
					try:
						template=json.loads(args.template)
					except:
						print("Template provided is not a path or a json object",file=sys.stderr)
						raise
				if hasattr(metrics, args.metric):  
					metric = getattr(metrics, args.metric)  # Get the function
				else:
					print(f"Error: metric {args.metric} does not exist")
					raise ParseException()
				if not args.num_threads or args.num_threads==1:
					coordinator=None
				else:
					from simcal.coordinators import ThreadPool
					coordinator=ThreadPool(args.num_threads)
				if args.verbose:
					verbosity = ["sim_error","parse_error"]
				else:
					verbosity = []
				algs=AlgManager(args.task_selection_schemes,args.worker_selection_schemes,args.num_cores_selection_schemes)
				
				simulator=SchedulingSimulator(args.simulator_path,template,calibration,algs,metric,coordinator,verbosity = verbosity)
				ret=simulator(state)
				print(ret)
			except ParseException:
				pass
			except Exception:
				print(traceback.format_exc())
				
		elif "calibration".startswith(sub):
			print("calibration not yet implemented",file=sys.stderr)
		elif "errorcorrection".startswith(sub) or "errorcorrection".startswith(sub) or sub == "ec":
			print("error correction not yet implemented",file=sys.stderr)
		else:
			print(f"subcommand \"{sys.argv[1]}\" not recognized",file=sys.stderr)
			raise
	except Exception:
		print(f"Usage: {sys.argv[0]} <scheduling | calibration | error_correction> ...\n\t Specify a sub command for the usage of that subcommand",file=sys.stderr)