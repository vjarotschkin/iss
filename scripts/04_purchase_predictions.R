library(readr)

set.seed(123)

load(here::here("data", 
                "processed", 
                "2023-07-10_cf_panel_df.RData")
     )
df <- trumpf_panel_data_set_r0
# Split data into training and testing sets (stratified by purchase_dummy)
data_split <- initial_split(df, prop = 0.8, strata = purchase_dummy)
train_data <- training(data_split)
test_data  <- testing(data_split)

# Define a recipe for preprocessing
purchase_recipe <- recipe(purchase_dummy ~ ., data = train_data) %>%
  # Impute missing numeric predictors
  step_impute_median(all_numeric(), -all_outcomes()) %>%
  # Impute missing categorical predictors
  step_impute_mode(all_nominal(), -all_outcomes()) %>%
  # Optionally, normalize numeric predictors
  step_normalize(all_numeric(), -all_outcomes())

# Specify a logistic regression model for classification
log_reg_spec <- logistic_reg() %>%
  set_engine("glm") %>%
  set_mode("classification")

# Create a workflow combining the recipe and the model
purchase_workflow <- workflow() %>%
  add_recipe(purchase_recipe) %>%
  add_model(log_reg_spec)

# Optionally, set up cross-validation
cv_folds <- vfold_cv(train_data, v = 5, strata = purchase_dummy)

# Fit the model using the training data
final_fit <- purchase_workflow %>% 
  fit(data = train_data)

# Evaluate on the testing set
test_predictions <- predict(final_fit, test_data, type = "prob") %>%
  bind_cols(test_data)

# Compute evaluation metrics (e.g., ROC AUC)
roc_auc(test_predictions, truth = purchase_dummy, .pred_1)