# DONE:

1) Submit tasks into parsl
2) Parsl will wait till a barrier task is called before actually submitting the task to the executor


# CURRENT:

1) Create a dictionary with information for each task Ex. # of children (immediate children), amount of input and output data, amount of computation to perform, pass it as an argument for the barrier function -> the executor pass it on to the interchange (scheduler)

# PLAN:
1) Implement scheduling algorithms into parsl

2) Set up docker configuration again (or find it on my old laptop)