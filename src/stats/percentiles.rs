use crate::stats::Mean;
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use statrs::statistics::Statistics;
use strum::{EnumCount, EnumIter, IntoEnumIterator};

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, EnumIter, EnumCount)]
pub enum Percentile {
    Min = 0,
    P1,
    P2,
    P5,
    P10,
    P25,
    P50,
    P75,
    P90,
    P95,
    P98,
    P99,
    P99_9,
    P99_99,
    Max,
}

impl Percentile {
    pub fn value(&self) -> f64 {
        match self {
            Percentile::Min => 0.0,
            Percentile::P1 => 1.0,
            Percentile::P2 => 2.0,
            Percentile::P5 => 5.0,
            Percentile::P10 => 10.0,
            Percentile::P25 => 25.0,
            Percentile::P50 => 50.0,
            Percentile::P75 => 75.0,
            Percentile::P90 => 90.0,
            Percentile::P95 => 95.0,
            Percentile::P98 => 98.0,
            Percentile::P99 => 99.0,
            Percentile::P99_9 => 99.9,
            Percentile::P99_99 => 99.99,
            Percentile::Max => 100.0,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Percentile::Min => "  Min   ",
            Percentile::P1 => "    1   ",
            Percentile::P2 => "    2   ",
            Percentile::P5 => "    5   ",
            Percentile::P10 => "   10   ",
            Percentile::P25 => "   25   ",
            Percentile::P50 => "   50   ",
            Percentile::P75 => "   75   ",
            Percentile::P90 => "   90   ",
            Percentile::P95 => "   95   ",
            Percentile::P98 => "   98   ",
            Percentile::P99 => "   99   ",
            Percentile::P99_9 => "   99.9 ",
            Percentile::P99_99 => "  99.99",
            Percentile::Max => "  Max   ",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Percentiles([Mean; Percentile::COUNT]);

impl Percentiles {
    const POPULATION_SIZE: usize = 100;

    /// Computes distribution percentiles without errors.
    /// Fast.
    pub fn compute(histogram: &Histogram<u64>, scale: f64) -> Percentiles {
        let mut result = Vec::with_capacity(Percentile::COUNT);
        for p in Percentile::iter() {
            result.push(Mean {
                n: Self::POPULATION_SIZE as u64,
                value: histogram.value_at_percentile(p.value()) as f64 * scale,
                std_err: None,
            });
        }
        assert_eq!(result.len(), Percentile::COUNT);
        Percentiles(result.try_into().unwrap())
    }

    /// Computes distribution percentiles with errors based on a HDR histogram.
    /// Caution: this is slow. Don't use it when benchmark is running!
    /// Errors are estimated by bootstrapping a larger population of histograms from the
    /// distribution determined by the original histogram and computing the standard error.   
    pub fn compute_with_errors(
        histogram: &Histogram<u64>,
        scale: f64,
        effective_sample_size: u64,
    ) -> Percentiles {
        let mut rng = SmallRng::from_rng(rand::thread_rng()).unwrap();

        let mut samples: Vec<[f64; Percentile::COUNT]> = Vec::with_capacity(Self::POPULATION_SIZE);
        for _ in 0..Self::POPULATION_SIZE {
            samples.push(percentiles(
                &bootstrap(&mut rng, histogram, effective_sample_size),
                scale,
            ))
        }

        let mut result = Vec::with_capacity(Percentile::COUNT);
        for p in Percentile::iter() {
            let std_err = samples.iter().map(|s| s[p as usize]).std_dev();
            result.push(Mean {
                n: Self::POPULATION_SIZE as u64,
                value: histogram.value_at_percentile(p.value()) as f64 * scale,
                std_err: Some(std_err),
            });
        }

        assert_eq!(result.len(), Percentile::COUNT);
        Percentiles(result.try_into().unwrap())
    }

    pub fn get(&self, percentile: Percentile) -> Mean {
        self.0[percentile as usize]
    }
}

/// Creates a new random histogram using another histogram as the distribution.
fn bootstrap(rng: &mut impl Rng, histogram: &Histogram<u64>, effective_n: u64) -> Histogram<u64> {
    let n = histogram.len();
    if n <= 1 {
        return histogram.clone();
    }
    let mut result =
        Histogram::new_with_bounds(histogram.low(), histogram.high(), histogram.sigfig()).unwrap();

    for bucket in histogram.iter_recorded() {
        let p = bucket.count_at_value() as f64 / n as f64;
        assert!(p > 0.0, "Probability must be greater than 0.0");
        let b = rand_distr::Binomial::new(effective_n, p).unwrap();
        let count: u64 = rng.sample(b);
        result.record_n(bucket.value_iterated_to(), count).unwrap()
    }
    result
}

fn percentiles(hist: &Histogram<u64>, scale: f64) -> [f64; Percentile::COUNT] {
    let mut percentiles = [0.0; Percentile::COUNT];
    for (i, p) in Percentile::iter().enumerate() {
        percentiles[i] = hist.value_at_percentile(p.value()) as f64 * scale;
    }
    percentiles
}

#[cfg(test)]
mod test {
    use crate::stats::percentiles::{Percentile, Percentiles};
    use assert_approx_eq::assert_approx_eq;
    use hdrhistogram::Histogram;
    use rand::Rng;
    use statrs::distribution::Uniform;

    #[test]
    fn test_zero_error() {
        let mut histogram = Histogram::<u64>::new(3).unwrap();
        for _ in 0..100000 {
            histogram.record(1000).unwrap();
        }

        let percentiles = Percentiles::compute_with_errors(&histogram, 1e-6, histogram.len());
        let median = percentiles.get(Percentile::P50);
        assert_approx_eq!(median.value, 0.001, 0.00001);
        assert_approx_eq!(median.std_err.unwrap(), 0.000, 1e-15);
    }

    #[test]
    fn test_min_max_error() {
        let mut histogram = Histogram::<u64>::new(3).unwrap();
        let d = Uniform::new(0.0, 1000.0).unwrap();
        const N: usize = 100000;
        for _ in 0..N {
            histogram
                .record(rand::thread_rng().sample(d).round() as u64)
                .unwrap();
        }

        let percentiles = Percentiles::compute_with_errors(&histogram, 1e-6, histogram.len());
        let min = percentiles.get(Percentile::Min);
        let max = percentiles.get(Percentile::Max);
        assert!(min.std_err.unwrap() < max.value / N as f64);
        assert!(max.std_err.unwrap() < max.value / N as f64);
    }
}
