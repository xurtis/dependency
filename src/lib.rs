//! Parallel dependency graph

mod error;
mod queue;

pub use error::Error;
type Result<T, E> = std::result::Result<T, Error<E>>;

use std::sync::{Arc, RwLock, RwLockReadGuard};

use queue::Task;

/// A target in the dependency graph
pub trait Target<E> {
    /// Get the status of the data
    fn status(&self) -> Status;

    /// Regenerate the data
    fn regenerate(&mut self) -> ::std::result::Result<(), E>;
}

/// The status for the data of a target
pub enum Status {
    /// The target must be re-evaluated before the data is ready
    Pending,
    /// The data is ready and the target does not need to be re-evaluated
    Ready,
}

/// A single node in the dependency graph
pub struct Node<T, E> {
    lock: Arc<RwLock<NodeBuilder<T, E>>>,
}

impl<T, E> Clone for Node<T, E> {
    fn clone(&self) -> Self {
        Node {
            lock: self.lock.clone(),
        }
    }
}

impl<T: Target<E> + 'static, E: 'static> Node<T, E> {
    /// Create a new dependency with an initial value
    pub fn new(value: T) -> NodeBuilder<T, E> {
        NodeBuilder {
            value,
            dependencies: vec![],
        }
    }

    /// Resolve the node and all of its dependencies
    pub fn resolve(&self) -> Result<NodeRef<T, E>, E> {
        use queue::Resolve;
        let task = self.clone();
        Resolve::resolve(task)?;
        NodeRef::try_from_node(self)
    }
}

impl<T: Target<E>, E> Task<E> for Node<T, E> {
    fn dependencies(&self) -> Result<Vec<Box<dyn Task<E>>>, E> {
        let node = self.lock.read()?;
        let mut dependencies = Vec::with_capacity(node.dependencies.len());

        for dependency in &node.dependencies {
            if let Status::Pending = dependency.status()? {
                dependencies.push(dependency.depend(self.clone()));
            }
        }

        Ok(dependencies)
    }

    fn process(&mut self) -> Result<(), E> {
        let mut node = self.lock.write()?;
        node.value.regenerate().map_err(Error::Transform)
    }
}

/// Builder used to add dependencies to a node
///
/// A node can only have dependencies added before it is completed and can be only added as a
/// dependency of other nodes after it has been fully built.
pub struct NodeBuilder<T, E> {
    value: T,
    dependencies: Vec<Box<dyn Depend<T, E>>>,
}

impl<T: 'static, E: 'static> NodeBuilder<T, E> {
    /// Build the node into a finalised node
    pub fn build(self) -> Node<T, E> {
        Node {
            lock: Arc::new(RwLock::new(self)),
        }
    }

    /// Add an external dependency
    pub fn depend<D: Target<E> + 'static>(
        mut self,
        dependency: &Node<D, E>,
        consume: impl Fn(&mut T, &D) -> Result<(), E> + 'static,
    ) -> Self {
        let dependency = Dependency::new(dependency.clone(), consume);
        self.dependencies.push(Box::new(dependency));
        self
    }
}

/// A reference to the inner value of a node through all locking mechanisms
pub struct NodeRef<'t, T, E> {
    node: RwLockReadGuard<'t, NodeBuilder<T, E>>,
}

impl<'t, T, E> NodeRef<'t, T, E> {
    fn try_from_node(node: &'t Node<T, E>) -> Result<Self, E> {
        let node = node.lock.read()?;
        Ok(NodeRef { node })
    }
}

impl<'t, T, E> ::std::ops::Deref for NodeRef<'t, T, E> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.node.value
    }
}

/// A reference to a dependency and how to apply it to a node
struct Dependency<T, E, D, F> {
    target: ::std::marker::PhantomData<T>,
    dependency: Node<D, E>,
    apply: Arc<F>,
}

impl<T, E, D, F> Dependency<T, E, D, F>
where
    F: Fn(&mut T, &D) -> Result<(), E>,
{
    fn new(dependency: Node<D, E>, apply: F) -> Self {
        let apply = Arc::new(apply);
        let target = ::std::marker::PhantomData;
        Dependency {
            target,
            dependency,
            apply,
        }
    }
}

/// Resolve a dependency and apply it to the dependant node
trait Depend<T, E> {
    fn depend(&self, target: Node<T, E>) -> Box<dyn queue::Task<E>>;

    fn status(&self) -> Result<Status, E>;
}

impl<T, E, D, F> Depend<T, E> for Dependency<T, E, D, F>
where
    T: 'static,
    E: 'static,
    D: 'static,
    F: 'static,
    D: Target<E>,
    F: Fn(&mut T, &D) -> Result<(), E>,
{
    fn depend(&self, target: Node<T, E>) -> Box<dyn queue::Task<E>> {
        let task = DependencyTask {
            target,
            dependency: self.dependency.clone(),
            apply: self.apply.clone(),
        };
        Box::new(task)
    }

    fn status(&self) -> Result<Status, E> {
        Ok(self.dependency.lock.read()?.value.status())
    }
}

/// A reference to a dependency and how to apply it to a node
struct DependencyTask<T, E, D, F> {
    target: Node<T, E>,
    dependency: Node<D, E>,
    apply: Arc<F>,
}

impl<T, E, D, F> queue::Task<E> for DependencyTask<T, E, D, F>
where
    D: Target<E> + 'static,
    E: 'static,
    F: Fn(&mut T, &D) -> Result<(), E>,
{
    fn dependencies(&self) -> Result<Vec<Box<dyn Task<E>>>, E> {
        // Depend on the next target rebuilding itself
        Ok(vec![Box::new(self.dependency.clone())])
    }

    fn process(&mut self) -> Result<(), E> {
        // As the dependency graphy should always effectively be a DAG, there should never be a
        // competing inversion of the locks here, i.e. anything accessing both of these
        // simultaneously will always access them in this order.
        let mut target = self.target.lock.write()?;
        let dependency = self.dependency.lock.read()?;

        (self.apply)(&mut target.value, &dependency.value)
    }
}
