.. TaskManager documentation master file, created by
   sphinx-quickstart on Thu Aug 25 14:08:23 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to TaskManager's documentation!
=======================================

Introduction:

TaskManager allows to develop generic workflows in Perl, potentially executable in various environments and with different degrees of parallelization. TaskManager can execute from two to thousands tasks in parallel, depending on the available resources, using "batch manager" as SLURM or LSF. The divide and conquer approach is particularly well adapted in the case of applying treatments on NGS data. It generates logs with the same structure in all execution environments that facilitate analysis and error recovery. TaskManager is developed using object-oriented programming that facilitate future evolution, adding functionality and new runtime environments. Thanks to TaskManager, the developer will always have the same way of implementing workflows.

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

