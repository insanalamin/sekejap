//! High-performance distance kernels with SIMD acceleration
//!
//! Provides optimized distance metrics for f32 vectors:
//! - L2 (Squared Euclidean)
//! - Dot Product
//! - Cosine Similarity (via normalization + dot)

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Trait for distance calculation
pub trait Distance {
    fn eval(a: &[f32], b: &[f32]) -> f32;
}

pub struct L2Distance;
pub struct DotProduct;
pub struct CosineDistance;

impl Distance for L2Distance {
    #[inline(always)]
    fn eval(a: &[f32], b: &[f32]) -> f32 {
        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "avx2",
            target_feature = "fma"
        ))]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return unsafe { l2_avx2(a, b) };
            }
        }

        // Fallback: Auto-vectorized Rust
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| {
                let diff = x - y;
                diff * diff
            })
            .sum()
    }
}

impl Distance for DotProduct {
    #[inline(always)]
    fn eval(a: &[f32], b: &[f32]) -> f32 {
        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "avx2",
            target_feature = "fma"
        ))]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return unsafe { dot_avx2(a, b) };
            }
        }

        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }
}

impl Distance for CosineDistance {
    #[inline(always)]
    fn eval(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

        if mag_a == 0.0 || mag_b == 0.0 {
            return 1.0;
        }

        let sim = dot / (mag_a * mag_b);
        1.0 - sim.max(-1.0).min(1.0)
    }
}

/// AVX2 Squared Euclidean Distance
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn l2_avx2(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let mut sum = _mm256_setzero_ps();

    let mut i = 0;
    while i + 8 <= n {
        unsafe {
            let va = _mm256_loadu_ps(a.as_ptr().add(i));
            let vb = _mm256_loadu_ps(b.as_ptr().add(i));
            let diff = _mm256_sub_ps(va, vb);
            sum = _mm256_fmadd_ps(diff, diff, sum);
        }
        i += 8;
    }

    let mut res = [0.0f32; 8];
    unsafe { _mm256_storeu_ps(res.as_mut_ptr(), sum) };

    let mut total = res.iter().sum::<f32>();

    // Remainder
    while i < n {
        let diff = a[i] - b[i];
        total += diff * diff;
        i += 1;
    }

    total
}

/// AVX2 Dot Product
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn dot_avx2(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let mut sum = _mm256_setzero_ps();

    let mut i = 0;
    while i + 8 <= n {
        unsafe {
            let va = _mm256_loadu_ps(a.as_ptr().add(i));
            let vb = _mm256_loadu_ps(b.as_ptr().add(i));
            sum = _mm256_fmadd_ps(va, vb, sum);
        }
        i += 8;
    }

    let mut res = [0.0f32; 8];
    unsafe { _mm256_storeu_ps(res.as_mut_ptr(), sum) };

    let mut total = res.iter().sum::<f32>();

    while i < n {
        total += a[i] * b[i];
        i += 1;
    }

    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_correctness() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        // (4-1)^2 + (5-2)^2 + (6-3)^2 = 9 + 9 + 9 = 27
        assert_eq!(L2Distance::eval(&a, &b), 27.0);
    }

    #[test]
    fn test_dot_correctness() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        assert_eq!(DotProduct::eval(&a, &b), 32.0);
    }
}
