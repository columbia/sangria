import math
from functools import reduce
import argparse


def log_product_no_collision(N, M):
    # Compute log of product of (1 - k/N) for k in 0..M-1 to avoid underflow
    return sum(math.log(1 - k / N) for k in range(M))


def collision_probability_variable_picks(N, P):
    total_prob = 0.0
    for x in range(P + 1):  # x = number of people who picked 2 elements
        M = P + x  # total number of picks
        binom_coeff = math.comb(P, x)
        prob_x = binom_coeff * (0.5**P)  # P(X = x) in Bin(P, 0.5)

        # Handle edge case: M > N → guaranteed collision
        if M > N:
            prob_collision = 1.0
        else:
            log_p_no_collision = log_product_no_collision(N, M)
            p_no_collision = math.exp(log_p_no_collision)
            prob_collision = 1 - p_no_collision

        total_prob += prob_x * prob_collision

    return total_prob


def find_P_for_target_probability(N, target_prob, tolerance=1e-5, max_P=1000):
    low, high = 1, max_P
    best_P = None

    while low <= high:
        mid = (low + high) // 2
        prob = collision_probability_variable_picks(N, mid)

        if abs(prob - target_prob) < tolerance:
            return mid
        elif prob < target_prob:
            low = mid + 1
        else:
            best_P = mid  # still track closest that overshoots
            high = mid - 1

    return best_P


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-N",
        required=False,
        type=int,
        help="The number of keys",
    )
    parser.add_argument(
        "-P",
        required=False,
        type=int,
        help="max concurrency",
    )
    parser.add_argument(
        "-T",
        required=False,
        type=float,
        help="target probability",
    )
    args = parser.parse_args()

    if args.N is not None and args.P is not None:
        print(
            f"Collision probability: {collision_probability_variable_picks(args.N, args.P):.6f}"
        )
    elif args.N is not None and args.T is not None:
        P = find_P_for_target_probability(args.N, args.T)
        print(f"P: {P}")
    # elif N is None and P is not None:
    #     N = find_N_for_target_probability(P, 0.5)
    #     print(f"N: {N}")
