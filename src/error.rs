//! Errors

use std::error;
use std::fmt::{self, Display, Formatter};

use std::sync::PoisonError;

/// Errors produced by the dependency queue
#[derive(Debug)]
pub enum Error<E> {
    /// Lock poisoned for queue notification
    LockPoison,
    /// Error during dependency transformation
    Transform(E),
}

impl<E: Display> Display for Error<E> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use Error::*;
        match self {
            LockPoison => write!(f, "Lock poisoned in dependency resolution"),
            Transform(err) => err.fmt(f),
        }
    }
}

impl<E: error::Error> error::Error for Error<E> {}

impl<T, E> From<PoisonError<T>> for Error<E> {
    fn from(_: PoisonError<T>) -> Self {
        Error::LockPoison
    }
}
