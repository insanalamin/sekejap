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
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return unsafe { l2_avx2(a, b) };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return unsafe { l2_neon(a, b) };
        }
        #[allow(unreachable_code)]
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
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return unsafe { dot_avx2(a, b) };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return unsafe { dot_neon(a, b) };
        }
        #[allow(unreachable_code)]
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }
}

impl Distance for CosineDistance {
    #[inline(always)]
    fn eval(a: &[f32], b: &[f32]) -> f32 {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return unsafe { cosine_avx2(a, b) };
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            return unsafe { cosine_neon(a, b) };
        }
        #[allow(unreachable_code)]
        {
            // Scalar fallback: single pass
            let mut dot = 0.0f32;
            let mut mag_a = 0.0f32;
            let mut mag_b = 0.0f32;
            for (x, y) in a.iter().zip(b.iter()) {
                dot += x * y;
                mag_a += x * x;
                mag_b += y * y;
            }
            let mag_a = mag_a.sqrt();
            let mag_b = mag_b.sqrt();
            if mag_a == 0.0 || mag_b == 0.0 {
                return 1.0;
            }
            let sim = dot / (mag_a * mag_b);
            1.0 - sim.max(-1.0).min(1.0)
        }
    }
}

// ============================================================================
// x86_64 AVX2 kernels
// ============================================================================

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

/// AVX2 single-pass CosineDistance (dot + mag_a + mag_b in one loop).
/// Previously missing — CosineDistance had no SIMD path on x86_64 at all.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn cosine_avx2(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let mut dot_sum = _mm256_setzero_ps();
    let mut mag_a_sum = _mm256_setzero_ps();
    let mut mag_b_sum = _mm256_setzero_ps();

    let mut i = 0;
    while i + 8 <= n {
        let va = _mm256_loadu_ps(a.as_ptr().add(i));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i));
        dot_sum = _mm256_fmadd_ps(va, vb, dot_sum);
        mag_a_sum = _mm256_fmadd_ps(va, va, mag_a_sum);
        mag_b_sum = _mm256_fmadd_ps(vb, vb, mag_b_sum);
        i += 8;
    }

    // Horizontal sum
    let mut dot_arr = [0.0f32; 8];
    let mut mag_a_arr = [0.0f32; 8];
    let mut mag_b_arr = [0.0f32; 8];
    _mm256_storeu_ps(dot_arr.as_mut_ptr(), dot_sum);
    _mm256_storeu_ps(mag_a_arr.as_mut_ptr(), mag_a_sum);
    _mm256_storeu_ps(mag_b_arr.as_mut_ptr(), mag_b_sum);

    let mut dot: f32 = dot_arr.iter().sum();
    let mut mag_a: f32 = mag_a_arr.iter().sum();
    let mut mag_b: f32 = mag_b_arr.iter().sum();

    // Scalar remainder
    while i < n {
        dot += a[i] * b[i];
        mag_a += a[i] * a[i];
        mag_b += b[i] * b[i];
        i += 1;
    }

    let mag_a = mag_a.sqrt();
    let mag_b = mag_b.sqrt();
    if mag_a == 0.0 || mag_b == 0.0 {
        return 1.0;
    }
    let sim = dot / (mag_a * mag_b);
    1.0 - sim.max(-1.0).min(1.0)
}

// ============================================================================
// aarch64 NEON kernels
// ============================================================================

/// NEON Squared Euclidean Distance
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn l2_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;
    let n = a.len();
    let mut sum = vdupq_n_f32(0.0);
    let mut i = 0;
    while i + 4 <= n {
        let va = vld1q_f32(a.as_ptr().add(i));
        let vb = vld1q_f32(b.as_ptr().add(i));
        let diff = vsubq_f32(va, vb);
        sum = vfmaq_f32(sum, diff, diff);
        i += 4;
    }
    let mut total = vaddvq_f32(sum);
    while i < n {
        let diff = a[i] - b[i];
        total += diff * diff;
        i += 1;
    }
    total
}

/// NEON Dot Product
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;
    let n = a.len();
    let mut sum = vdupq_n_f32(0.0);
    let mut i = 0;
    while i + 4 <= n {
        let va = vld1q_f32(a.as_ptr().add(i));
        let vb = vld1q_f32(b.as_ptr().add(i));
        sum = vfmaq_f32(sum, va, vb);
        i += 4;
    }
    let mut total = vaddvq_f32(sum);
    while i < n {
        total += a[i] * b[i];
        i += 1;
    }
    total
}

/// NEON single-pass CosineDistance.
/// 128-dim / 4-wide = exactly 32 iterations, no remainder.
/// Three accumulators (dot, mag_a, mag_b) fused into one loop.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn cosine_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;
    let n = a.len();
    let mut dot_sum = vdupq_n_f32(0.0);
    let mut mag_a_sum = vdupq_n_f32(0.0);
    let mut mag_b_sum = vdupq_n_f32(0.0);
    let mut i = 0;
    while i + 4 <= n {
        let va = vld1q_f32(a.as_ptr().add(i));
        let vb = vld1q_f32(b.as_ptr().add(i));
        dot_sum = vfmaq_f32(dot_sum, va, vb);
        mag_a_sum = vfmaq_f32(mag_a_sum, va, va);
        mag_b_sum = vfmaq_f32(mag_b_sum, vb, vb);
        i += 4;
    }
    let mut dot = vaddvq_f32(dot_sum);
    let mut mag_a = vaddvq_f32(mag_a_sum);
    let mut mag_b = vaddvq_f32(mag_b_sum);
    while i < n {
        dot += a[i] * b[i];
        mag_a += a[i] * a[i];
        mag_b += b[i] * b[i];
        i += 1;
    }
    let mag_a = mag_a.sqrt();
    let mag_b = mag_b.sqrt();
    if mag_a == 0.0 || mag_b == 0.0 {
        return 1.0;
    }
    let sim = dot / (mag_a * mag_b);
    1.0 - sim.max(-1.0).min(1.0)
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

    #[test]
    fn test_cosine_correctness_128dim() {
        let a: Vec<f32> = (0..128).map(|i| (i as f32) * 0.01 + 0.1).collect();
        let b: Vec<f32> = (0..128).map(|i| (127 - i) as f32 * 0.01 + 0.1).collect();

        // Scalar reference
        let mut dot = 0.0f32;
        let mut mag_a = 0.0f32;
        let mut mag_b = 0.0f32;
        for (x, y) in a.iter().zip(b.iter()) {
            dot += x * y;
            mag_a += x * x;
            mag_b += y * y;
        }
        let expected = 1.0 - (dot / (mag_a.sqrt() * mag_b.sqrt()));

        let result = CosineDistance::eval(&a, &b);
        assert!(
            (result - expected).abs() < 1e-4,
            "SIMD vs scalar mismatch: got {result}, expected {expected}"
        );
    }
}
