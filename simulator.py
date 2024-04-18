""" 
Project 1 (Discrete Time Event Simulator)
@author: Caleb Alvarez
"""
import sys
import heapq
import random
import math

class Process:
    def __init__(self, id, arrivalTime, serviceTime):
        self.id = id
        self.arrivalTime = arrivalTime
        self.serviceTime = serviceTime
        self.remainingTime = serviceTime
        self.completionTime = 0

class Event:
    def __init__(self, type, time, processID):
        self.type = type
        self.time = time
        self.processID = processID

    # Define event priority by earliest time
    def __lt__(self, other):
        return self.time < other.time

class Simulator:
    def __init__(self, scheduler, lamb, avg_serviceTime, quantum):
        # User provided variables
        self.scheduler = scheduler
        self.lamb = lamb
        self.avg_serviceTime = avg_serviceTime
        self.quantum = quantum

        # Object Queues
        self.eventQ = []
        self.readyQ = []
        self.completedProcesses = []

        # Status Variables
        self.clock = 0
        self.processCounter = 0
        self.busy = False
        self.currentProcess = None
        self.checks = 0

        # Metrics variables
        self.total_turnaroundTime = 0
        self.total_cpuUtilization = 0
        self.total_processQ = 0

    # Generates an arrival time based on poisson distribution of lambda
    def generate_arrivalTime(self):
        return -math.log(1.0 - random.random()) / self.lamb

    # Generates a service time based on exponential distribution of average service time
    def generate_serviceTime(self):
        return -self.avg_serviceTime * math.log(random.random())

    # Generates an ARRIVAL event and pushes it to eventQ
    def generate_arrivalEvent(self):
        if len(self.completedProcesses) < 10000:
            arrivalTime = self.generate_arrivalTime() + self.clock
            self.processCounter += 1
            heapq.heappush(self.eventQ, Event('ARRIVAL', arrivalTime, self.processCounter))
    
    # Generates a STATUS event and pushes it to eventQ
    def generate_statusEvent(self):
        if len(self.completedProcesses) < 10000:
            heapq.heappush(self.eventQ, Event('STATUS', self.clock + (self.avg_serviceTime / 2), None))

    def generate_timeSlice(self):
        if len(self.completedProcesses) < 10000:
            heapq.heappush(self.eventQ, Event('TIMESLICE', self.clock + self.quantum, None))

    # Handles a STATUS event
    def handleStatus(self):
        self.checks += 1
        self.total_processQ += len(self.readyQ)
    
    # Executes a process under a period quantum else execute as non-preemptive
    def execute_timeSlice(self, process):
        self.busy = True
        self.currentProcess = process
        if self.currentProcess.remainingTime <= self.quantum:
            self.execute_nonPreemptive(process)
        else:
            self.generate_timeSlice()

    # Executes any non-preemptive process and creates a DEPARTURE event for said process
    def execute_nonPreemptive(self, process):
        self.busy = True
        process.completionTime = self.clock + process.remainingTime
        self.currentProcess = process
        heapq.heappush(self.eventQ, Event('DEPARTURE', process.completionTime, process.id))
    
    # Handles preemption of a process and executes the next process if any,
    # otherwise continues to execute the current process
    def handlePreemption(self):
        self.currentProcess.remainingTime -= self.quantum
        self.readyQ.append(self.currentProcess)
        self.execute_timeSlice(self.readyQ.pop(0))

    # Updates simulator metrics and executes the next process if any
    def handleDeparture(self):
        self.busy = False
        self.currentProcess.remainingTime = 0
        self.completedProcesses.append(self.currentProcess)

        self.total_cpuUtilization += self.currentProcess.serviceTime
        self.total_turnaroundTime += self.currentProcess.completionTime - self.currentProcess.arrivalTime

        if len(self.completedProcesses) == 10000:
            return
        elif self.readyQ and (self.scheduler == 'HRRN' or 'FCFS'):
            self.execute_nonPreemptive(self.readyQ.pop(0))
        elif self.readyQ and self.scheduler == 'SRTF':
            self.execute_nonPreemptive(self.readyQ.pop(0))
        elif self.readyQ and self.scheduler == 'RR':
            self.execute_timeSlice(self.readyQ.pop(0))  
    
    # Sends ARRIVAL event to correct scheduler
    def handleArrival(self, event):
        if self.scheduler == 'FCFS':
            self.FCFS(event)
        elif self.scheduler == 'SRTF':
            self.SRTF(event)
        elif self.scheduler == 'HRRN':
            self.HRRN(event)
        elif self.scheduler == 'RR':
            self.RR(event)

    # Handles FCFS ARRIVAL event and executes its associated process
    def FCFS(self, event):
        newProcess = Process(event.processID, event.time, self.generate_serviceTime())
        if self.busy == False:
            self.execute_nonPreemptive(newProcess)
        else:
            self.readyQ.append(newProcess)
    
    # Handles SRTF ARRIVAL event and executes its associated process
    def SRTF(self, event):
        newProcess = Process(event.processID, event.time, self.generate_serviceTime())
        if self.busy == False:
            self.execute_nonPreemptive(newProcess)
        # If the current processes remaining time is less than the new process, it is interrupted pushed to readyQ
        elif newProcess.remainingTime < self.currentProcess.remainingTime - (self.clock - (self.currentProcess.arrivalTime + self.currentProcess.serviceTime - self.currentProcess.remainingTime)):
            time_elapsed = self.clock - (self.currentProcess.arrivalTime + self.currentProcess.serviceTime - self.currentProcess.remainingTime)
            self.currentProcess.remainingTime -= time_elapsed

            # Interrupt process currently running
            self.interrupt()
            self.readyQ.append(self.currentProcess)
            self.readyQ.sort(key=lambda x: x.remainingTime)
            self.execute_nonPreemptive(newProcess)
        else:
            self.readyQ.append(newProcess)
            self.readyQ.sort(key=lambda x: x.remainingTime)

    # Interrups the currently running process
    def interrupt(self):
        location = 0
        for event in self.eventQ:
            if (event.type == 'DEPARTURE') and (event.processID == self.currentProcess.id):
                self.eventQ.pop(location)
            location += 1
    
    # Handles HRRN ARRIVAL event and executes its associated process
    def HRRN(self, event):
        newProcess = Process(event.processID, event.time, self.generate_serviceTime())
        if self.busy == False:
            self.execute_nonPreemptive(newProcess)
        else:
            self.readyQ.append(newProcess)
            self.readyQ.sort(key=lambda x: (self.clock - x.arrivalTime + x.remainingTime) / x.remainingTime, reverse=True)
    
    # Handles RR ARRIVAL event and executes its associated process
    def RR(self, event):
        newProcess = Process(event.processID, event.time, self.generate_serviceTime())
        if self.busy == False:
            self.execute_timeSlice(newProcess)
        else:
            self.readyQ.append(newProcess)
    
    # Runs the simulation
    def runSimulation(self):
        # Generate all Processes and add them to the event queue
        self.generate_arrivalEvent()
        self.generate_statusEvent()

        # Handle events from the start of the queue based on type
        while self.eventQ and len(self.completedProcesses) < 10000:
            event = heapq.heappop(self.eventQ)
            self.clock = event.time

            if event.type == 'ARRIVAL':
                self.generate_arrivalEvent()
                self.handleArrival(event)
            elif event.type == 'DEPARTURE':
                self.handleDeparture()
            elif event.type == 'TIMESLICE':
                self.handlePreemption()
            elif event.type == 'STATUS':
                self.generate_statusEvent()
                self.handleStatus()

    # Outputs Calculated Performance Metrics of simulation to output file
    def outputMetrics(self):
        # Calculates Performance Metrics
        avg_turnaroundTime = round(self.total_turnaroundTime / self.processCounter, 2)
        avg_throughput = round(len(self.completedProcesses)/ self.clock, 2)
        avg_cpuUtilization = round(self.total_cpuUtilization / self.clock, 2)
        avg_processesInQ = round(self.total_processQ / self.checks, 2)

        #Output Metrics
        print(f'Average Turnaround: {avg_turnaroundTime}')
        print(f'Average Throughput: {avg_throughput}')
        print(f'Average CPU Utilization: {avg_cpuUtilization}')
        print(f'Average # of Process in Q: {avg_processesInQ}')   

#Takes four arguments and creates the simulation to be run
if __name__ == "__main__":
    type = int(sys.argv[1])
    rate = int(sys.argv[2])
    serviceTime = float(sys.argv[3])
    if len(sys.argv) >= 5:
        quantum = float(sys.argv[4])
    else:
        quantum = 0

    if type == 1: scheduler = 'FCFS'
    if type == 2: scheduler = 'SRTF'
    if type == 3: scheduler = 'HRRN'
    if type == 4: scheduler = 'RR'

    simulator = Simulator(scheduler, rate, serviceTime, quantum)
    simulator.runSimulation()
    simulator.outputMetrics()