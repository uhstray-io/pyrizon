# define a class to manage the workflow of a data processing pipeline, including the following: sources, targets, steps, tasks, transformations, tests, and dependencies

from typing import Dict, List, Callable, Any, Optional
from datetime import datetime
from enum import Enum
import logging
from dataclasses import dataclass
import networkx as nx

class TaskStatus(Enum):
    """Enumeration of possible task statuses in the pipeline."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class DataSource:
    """Represents a data source in the pipeline."""
    name: str
    connection_string: str
    type: str
    schema: Optional[Dict] = None
    
@dataclass
class DataTarget:
    """Represents a data target/destination in the pipeline."""
    name: str
    connection_string: str
    type: str
    schema: Optional[Dict] = None

class Task:
    """Represents an individual task in the pipeline."""
    def __init__(
        self,
        name: str,
        function: Callable,
        dependencies: List[str] = None,
        retry_count: int = 3,
        timeout: int = 3600
    ):
        self.name = name
        self.function = function
        self.dependencies = dependencies or []
        self.retry_count = retry_count
        self.timeout = timeout
        self.status = TaskStatus.PENDING
        self.start_time = None
        self.end_time = None
        self.error = None

    def execute(self, *args, **kwargs) -> Any:
        """Execute the task function with given arguments."""
        self.start_time = datetime.now()
        self.status = TaskStatus.RUNNING
        
        try:
            result = self.function(*args, **kwargs)
            self.status = TaskStatus.COMPLETED
            return result
        except Exception as e:
            self.status = TaskStatus.FAILED
            self.error = str(e)
            raise
        finally:
            self.end_time = datetime.now()

class DataPipeline:
    """
    Main class for managing the data processing pipeline workflow.
    
    This class implements a directed acyclic graph (DAG) based workflow system
    that handles task dependencies, data transformations, and validation tests.
    """
    
    def __init__(self, name: str):
        self.name = name
        self.sources: Dict[str, DataSource] = {}
        self.targets: Dict[str, DataTarget] = {}
        self.tasks: Dict[str, Task] = {}
        self.transformations: Dict[str, Callable] = {}
        self.tests: Dict[str, Callable] = {}
        self.dag = nx.DiGraph()
        self.logger = logging.getLogger(name)

    def add_source(self, source: DataSource) -> None:
        """Add a data source to the pipeline."""
        self.sources[source.name] = source
        
    def add_target(self, target: DataTarget) -> None:
        """Add a data target to the pipeline."""
        self.targets[target.name] = target

    def add_task(self, task: Task) -> None:
        """
        Add a task to the pipeline and update the DAG with its dependencies.
        """
        self.tasks[task.name] = task
        self.dag.add_node(task.name)
        
        for dep in task.dependencies:
            if dep not in self.tasks:
                raise ValueError(f"Dependency task '{dep}' not found")
            self.dag.add_edge(dep, task.name)
            
        # Verify that the graph remains acyclic
        if not nx.is_directed_acyclic_graph(self.dag):
            self.dag.remove_node(task.name)
            raise ValueError("Adding this task would create a cycle in the pipeline")

    def add_transformation(self, name: str, transform_fn: Callable) -> None:
        """Add a data transformation function to the pipeline."""
        self.transformations[name] = transform_fn

    def add_test(self, name: str, test_fn: Callable) -> None:
        """Add a validation test to the pipeline."""
        self.tests[name] = test_fn

    def validate_pipeline(self) -> bool:
        """
        Validate the pipeline configuration and structure.
        Returns True if valid, raises exceptions otherwise.
        """
        # Check for cycles in the DAG
        if not nx.is_directed_acyclic_graph(self.dag):
            raise ValueError("Pipeline contains circular dependencies")
            
        # Verify all dependencies exist
        for task in self.tasks.values():
            for dep in task.dependencies:
                if dep not in self.tasks:
                    raise ValueError(f"Task '{task.name}' depends on non-existent task '{dep}'")
        
        # Verify sources and targets are properly configured
        for source in self.sources.values():
            if not source.connection_string:
                raise ValueError(f"Source '{source.name}' missing connection string")
                
        for target in self.targets.values():
            if not target.connection_string:
                raise ValueError(f"Target '{target.name}' missing connection string")
        
        return True

    def execute(self) -> Dict[str, TaskStatus]:
        """
        Execute the pipeline in dependency order.
        Returns a dictionary of task names and their final statuses.
        """
        self.validate_pipeline()
        execution_order = list(nx.topological_sort(self.dag))
        results = {}

        for task_name in execution_order:
            task = self.tasks[task_name]
            
            # Check if dependencies completed successfully
            deps_ok = all(
                self.tasks[dep].status == TaskStatus.COMPLETED
                for dep in task.dependencies
            )
            
            if not deps_ok:
                task.status = TaskStatus.SKIPPED
                continue

            try:
                # Execute any associated tests before task execution
                self._run_tests(task_name)
                
                # Execute the task
                task.execute()
                
                # Run transformations if any are associated with this task
                if task_name in self.transformations:
                    self.transformations[task_name](results.get(task_name))
                    
            except Exception as e:
                self.logger.error(f"Task '{task_name}' failed: {str(e)}")
                raise

            results[task_name] = task.status

        return results

    def _run_tests(self, task_name: str) -> None:
        """Run validation tests associated with a task."""
        if task_name in self.tests:
            test_fn = self.tests[task_name]
            try:
                test_fn()
            except Exception as e:
                raise ValueError(f"Validation test failed for task '{task_name}': {str(e)}")

    def get_task_status(self, task_name: str) -> TaskStatus:
        """Get the current status of a specific task."""
        if task_name not in self.tasks:
            raise ValueError(f"Task '{task_name}' not found")
        return self.tasks[task_name].status

    def reset(self) -> None:
        """Reset the pipeline state, clearing all task statuses."""
        for task in self.tasks.values():
            task.status = TaskStatus.PENDING
            task.start_time = None
            task.end_time = None
            task.error = None