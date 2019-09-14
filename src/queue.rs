//! Parallel work queues of dependent jobs

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

use crate::Result;

/// A work queue
pub struct Queue<E> {
    jobs: Mutex<Vec<Node<E>>>,
}

impl<E> Queue<E> {
    /// Create a new queue to resolve all dependencies for a task
    pub fn new(task: Box<dyn Task<E>>) -> Result<Self, E> {
        let mut registered = VecDeque::new();
        let mut jobs = vec![];

        registered.push_back(Registered::new(task));

        while let Some(task) = registered.pop_front() {
            let (task, dependencies) = task.prepare()?;
            registered.extend(dependencies);
            jobs.push(task);
        }

        Ok(Queue {
            jobs: Mutex::new(jobs),
        })
    }

    /// Resolve all dependent tasks in parallel
    pub fn resolve(self) -> Result<(), E> {
        // TODO: do this in parallel

        while let Some(mut task) = self.jobs.lock()?.pop() {
            task.process()?;
        }

        Ok(())
    }
}

/// A task with dependencies
pub trait Task<E> {
    /// List any dependencies of the task
    fn dependencies(&self) -> Result<Vec<Box<dyn Task<E>>>, E>;

    /// Complete the task
    fn process(&mut self) -> Result<(), E>;
}

/// Resolution of a task with dependencies
pub trait Resolve<E>: Task<E> + Sized + 'static {
    /// Resolve a task and all of its dependencies
    fn resolve(self) -> Result<(), E> {
        Queue::new(Box::new(self))?.resolve()
    }
}

impl<T: Task<E> + 'static, E> Resolve<E> for T {}

/// A job node that has been registered in the queue
struct Registered<E> {
    task: Box<dyn Task<E>>,
    release: Notification,
}

impl<E> Registered<E> {
    /// Create a new task node
    fn new(task: Box<dyn Task<E>>) -> Self {
        Registered {
            task,
            release: Notification::new(),
        }
    }

    /// Get all of the dependencies of the task as tasks
    fn prepare(self) -> Result<(Node<E>, impl Iterator<Item = Registered<E>>), E> {
        let Registered { task, release } = self;
        let mut notifications = vec![];
        let mut dependencies = vec![];

        for dependency in task.dependencies()? {
            let dependency = Registered::new(dependency);
            notifications.push(dependency.release.clone());
            dependencies.push(dependency);
        }

        let node = Node {
            task,
            release,
            notifications,
        };

        Ok((node, dependencies.into_iter()))
    }
}

/// A job node that has had all of its dependencies registered
struct Node<E> {
    task: Box<dyn Task<E>>,
    release: Notification,
    notifications: Vec<Notification>,
}

impl<E> Node<E> {
    fn process(&mut self) -> Result<(), E> {
        for notification in &self.notifications {
            notification.wait()?;
        }

        self.task.process()?;

        self.release.signal()
    }
}

/// Simple binary notifications
#[derive(Clone)]
struct Notification(Arc<(Mutex<bool>, Condvar)>);

impl Notification {
    fn new() -> Self {
        Notification(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn wait<E>(&self) -> Result<(), E> {
        let arc = &self.0;
        while let false = *(arc.1.wait(arc.0.lock()?)?) {}

        Ok(())
    }

    fn signal<E>(&self) -> Result<(), E> {
        let arc = &self.0;
        let mut value = arc.0.lock()?;
        *value = true;
        arc.1.notify_all();

        Ok(())
    }
}
