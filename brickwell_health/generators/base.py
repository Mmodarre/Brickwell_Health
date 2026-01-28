"""
Base generator class for Brickwell Health Simulator.

Provides common functionality for all data generators.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Any
from uuid import UUID

import numpy as np
from numpy.random import Generator as RNG

from brickwell_health.reference.loader import ReferenceDataLoader

T = TypeVar("T")


class BaseGenerator(ABC, Generic[T]):
    """
    Abstract base class for data generators.

    Provides common utilities for random generation and reference data access.

    Usage:
        class MyGenerator(BaseGenerator[MyModel]):
            def generate(self, **kwargs) -> MyModel:
                ...
    """

    def __init__(self, rng: RNG, reference: ReferenceDataLoader):
        """
        Initialize the generator.

        Args:
            rng: NumPy random number generator
            reference: Reference data loader
        """
        self.rng = rng
        self.reference = reference

    @abstractmethod
    def generate(self, **kwargs: Any) -> T:
        """
        Generate a single entity.

        Args:
            **kwargs: Generation parameters

        Returns:
            Generated entity
        """
        pass

    def generate_uuid(self) -> UUID:
        """
        Generate a random UUID using the RNG for reproducibility.

        Returns:
            Random UUID
        """
        random_bytes = bytearray(self.rng.bytes(16))
        # Set version 4 (random) UUID bits
        random_bytes[6] = (random_bytes[6] & 0x0F) | 0x40
        random_bytes[8] = (random_bytes[8] & 0x3F) | 0x80
        return UUID(bytes=bytes(random_bytes))

    def choice(
        self,
        options: list[Any],
        weights: list[float] | None = None,
    ) -> Any:
        """
        Make a weighted random choice.

        Args:
            options: List of options to choose from
            weights: Optional weights (will be normalized)

        Returns:
            Chosen option
        """
        if not options:
            raise ValueError("Cannot choose from empty list")

        if weights:
            weights_arr = np.array(weights, dtype=float)
            weights_arr = weights_arr / weights_arr.sum()  # Normalize
            idx = self.rng.choice(len(options), p=weights_arr)
        else:
            idx = self.rng.integers(0, len(options))

        return options[idx]

    def choice_from_dict(self, distribution: dict[str, float]) -> str:
        """
        Choose from a dictionary distribution.

        Args:
            distribution: Dict mapping options to weights

        Returns:
            Chosen option key
        """
        options = list(distribution.keys())
        weights = list(distribution.values())
        return self.choice(options, weights)

    def uniform(self, low: float, high: float) -> float:
        """
        Generate uniform random number.

        Args:
            low: Lower bound
            high: Upper bound

        Returns:
            Random float in [low, high)
        """
        return self.rng.uniform(low, high)

    def uniform_int(self, low: int, high: int) -> int:
        """
        Generate uniform random integer.

        Args:
            low: Lower bound (inclusive)
            high: Upper bound (exclusive)

        Returns:
            Random integer in [low, high)
        """
        return int(self.rng.integers(low, high))

    def normal(self, mean: float, std: float) -> float:
        """
        Generate normal (Gaussian) random number.

        Args:
            mean: Mean of distribution
            std: Standard deviation

        Returns:
            Random float from N(mean, std^2)
        """
        return self.rng.normal(mean, std)

    def lognormal(self, mean: float, sigma: float) -> float:
        """
        Generate log-normal random number.

        Args:
            mean: Mean of underlying normal distribution
            sigma: Standard deviation of underlying normal distribution

        Returns:
            Random float from LogN(mean, sigma^2)
        """
        return self.rng.lognormal(mean, sigma)

    def poisson(self, lam: float) -> int:
        """
        Generate Poisson random number.

        Args:
            lam: Rate parameter (lambda)

        Returns:
            Random integer from Poisson(lam)
        """
        return int(self.rng.poisson(lam))

    def exponential(self, scale: float) -> float:
        """
        Generate exponential random number.

        Args:
            scale: Scale parameter (1/rate)

        Returns:
            Random float from Exp(1/scale)
        """
        return self.rng.exponential(scale)

    def bernoulli(self, p: float) -> bool:
        """
        Generate Bernoulli random variable.

        Args:
            p: Probability of True

        Returns:
            True with probability p
        """
        return self.rng.random() < p

    def beta(self, a: float, b: float) -> float:
        """
        Generate beta random number.

        Args:
            a: Alpha parameter
            b: Beta parameter

        Returns:
            Random float from Beta(a, b)
        """
        return self.rng.beta(a, b)

    def shuffle(self, items: list[Any]) -> list[Any]:
        """
        Shuffle a list in place and return it.

        Args:
            items: List to shuffle

        Returns:
            Shuffled list (same object)
        """
        self.rng.shuffle(items)
        return items

    def sample(self, population: list[Any], k: int) -> list[Any]:
        """
        Sample k items without replacement.

        Args:
            population: List to sample from
            k: Number of items to sample

        Returns:
            List of k sampled items
        """
        indices = self.rng.choice(len(population), size=min(k, len(population)), replace=False)
        return [population[i] for i in indices]
