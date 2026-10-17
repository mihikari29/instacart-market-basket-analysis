"""Pure provenance tests for prior-only official-train Hybrid evaluation."""

import unittest

from src.module4.evaluation import (
    PRIOR_ONLY_HYBRID_LAMBDAS,
    prior_only_hybrid_evaluation_provenance,
    validate_prior_only_hybrid_evaluation,
)


class PriorOnlyHybridProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.receipt = {
            "inputs": {
                "order_products_prior": {"sha256": "a" * 64},
                "order_products_train": {"sha256": "b" * 64},
                "products": {"sha256": "c" * 64},
            },
            "receipt_sha256": "d" * 64,
        }

    def test_provenance_records_split_hashes_and_frequency_proxy_limit(self):
        result = prior_only_hybrid_evaluation_provenance(
            self.receipt,
            prior_eval_set="prior",
            truth_eval_set="train",
            official_train_used_for_fit=False,
            official_train_used_for_tuning=False,
            mixed_module1_event_feed_used=False,
        )

        self.assertEqual(result["status"], "provenance_validated")
        self.assertEqual(result["prior_source_sha256"], "a" * 64)
        self.assertEqual(result["official_train_source_sha256"], "b" * 64)
        self.assertEqual(result["product_catalog_source_sha256"], "c" * 64)
        self.assertEqual(result["source_receipt_sha256"], "d" * 64)
        self.assertFalse(result["official_train_used_for_fit"])
        self.assertFalse(result["official_train_used_for_tuning"])
        self.assertIn("not claimed", result["streaming_trend_equivalence"])
        self.assertEqual(PRIOR_ONLY_HYBRID_LAMBDAS, (0.0, 0.5, 0.7, 0.9, 1.0))

    def test_mixed_event_feed_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "mixed Module 1"):
            prior_only_hybrid_evaluation_provenance(
                self.receipt,
                prior_eval_set="prior",
                truth_eval_set="train",
                official_train_used_for_fit=False,
                official_train_used_for_tuning=False,
                mixed_module1_event_feed_used=True,
            )

    def test_wrong_split_or_train_tuning_use_is_rejected(self):
        for values in (
            {"prior_eval_set": "prior_and_train", "truth_eval_set": "train"},
            {"prior_eval_set": "prior", "truth_eval_set": "prior"},
        ):
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, "requires prior-only"):
                prior_only_hybrid_evaluation_provenance(
                    self.receipt,
                    **values,
                    official_train_used_for_fit=False,
                    official_train_used_for_tuning=False,
                    mixed_module1_event_feed_used=False,
                )
        with self.assertRaisesRegex(ValueError, "cannot be used for fitting or tuning"):
            prior_only_hybrid_evaluation_provenance(
                self.receipt,
                prior_eval_set="prior",
                truth_eval_set="train",
                official_train_used_for_fit=True,
                official_train_used_for_tuning=False,
                mixed_module1_event_feed_used=False,
            )

    def test_acceptance_gate_requires_matching_receipts_cohorts_and_sweep(self):
        provenance = prior_only_hybrid_evaluation_provenance(
            self.receipt,
            prior_eval_set="prior",
            truth_eval_set="train",
            official_train_used_for_fit=False,
            official_train_used_for_tuning=False,
            mixed_module1_event_feed_used=False,
        )
        methods = {
            "als",
            "trend_only_prior_frequency",
            "global_popularity",
            "personal_frequency",
            *(f"hybrid_lambda_{value:.1f}".replace(".", "_") for value in PRIOR_ONLY_HYBRID_LAMBDAS),
        }
        metric_row = {
            "status": "evaluated",
            "evaluated_users": 10,
            "precision_at_k": 0.1,
            "recall_at_k": 0.2,
            "map_at_k": 0.15,
            "ndcg_at_k": 0.25,
        }
        evaluation = {
            "status": "evaluated",
            "run_id": "fixture-run",
            "model_version": "als-fixture-v1",
            "source_code_sha256": "e" * 64,
            "als_parameters": {"rank": 16},
            "catalog_size": 20,
            "cold_start_summary": {"users_without_user_factors": 1},
            "provenance": provenance,
            "candidate_policy": {
                "output_top_k": 20,
                "epsilon": 1e-12,
                "output_order": "descending unrounded hybrid score, then ascending product_id",
                "lambda_values_preregistered": list(PRIOR_ONLY_HYBRID_LAMBDAS),
                "lambda_selection": "fixed sweep; official-train truth is not used to select a value",
            },
            "cohorts": {
                name: {
                    "metrics": {
                        method: {k: dict(metric_row) for k in ("5", "10", "20")}
                        for method in methods
                    }
                }
                for name in ("all_official_train_users", "warm_users_and_all_truth_items")
            },
        }

        validate_prior_only_hybrid_evaluation(evaluation, self.receipt)

        evaluation["cohorts"]["all_official_train_users"]["metrics"].pop("hybrid_lambda_0_5")
        with self.assertRaisesRegex(ValueError, "cohort all_official_train_users is incomplete"):
            validate_prior_only_hybrid_evaluation(evaluation, self.receipt)


if __name__ == "__main__":
    unittest.main()
