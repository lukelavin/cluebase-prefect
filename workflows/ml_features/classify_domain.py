from logging import getLogger
from typing import Tuple

import torch
from huggingface_hub import PyTorchModelHubMixin
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger
from pymongo import MongoClient, UpdateOne
from torch import nn
from transformers import AutoConfig, AutoModel, AutoTokenizer

file_logger = getLogger(__name__)


class CustomModel(nn.Module, PyTorchModelHubMixin):
    def __init__(self, config):
        super(CustomModel, self).__init__()
        self.model = AutoModel.from_pretrained(config["base_model"])
        self.dropout = nn.Dropout(config["fc_dropout"])
        self.fc = nn.Linear(self.model.config.hidden_size, len(config["id2label"]))

    def forward(self, input_ids, attention_mask):
        features = self.model(
            input_ids=input_ids, attention_mask=attention_mask
        ).last_hidden_state
        dropped = self.dropout(features)
        outputs = self.fc(dropped)
        return torch.softmax(outputs[:, 0, :], dim=1)


@flow
def classify_domains(overwrite=False):
    model_config, tokenizer, model = setup_model()

    predict_all_domains(model_config, tokenizer, model, overwrite=overwrite)


@task
def setup_model() -> Tuple[AutoConfig, AutoTokenizer, CustomModel]:
    logger = get_run_logger()
    # logger = logging.getLogger(__name__)
    logger.info("Setting up models")
    # Setup configuration and model
    id2label = {
        0: "UNUSED",
        1: "Arts and Entertainment",
        10: "Health",
        11: "Hobbies and Leisure",
        12: "Home and Garden",
        13: "Internet and Telecom",
        14: "Jobs and Education",
        15: "Law and Government",
        16: "News",
        17: "UNUSED",
        18: "People and Society",
        19: "Pets and Animals",
        2: "Autos and Vehicles",
        20: "UNUSED",
        21: "Science",
        22: "UNUSED",
        23: "Shopping",
        24: "Sports",
        25: "Travel and Transportation",
        3: "Beauty and Fitness",
        4: "Books and Literature",
        5: "Business and Industrial",
        6: "Computers and Electronics",
        7: "Finance",
        8: "Food and Drink",
        9: "Games",
    }
    config = AutoConfig.from_pretrained("nvidia/domain-classifier")
    config.id2label = id2label
    tokenizer = AutoTokenizer.from_pretrained("nvidia/domain-classifier")
    model = CustomModel.from_pretrained("nvidia/domain-classifier")

    if torch.cuda.is_available():
        model = model.cuda()

    logger.info("Completed model set-up")
    return config, tokenizer, model


@task
def predict_all_domains(
    model_config,
    tokenizer,
    model,
    mongo_secret_block="mongo-connection-string",
    overwrite=False,
):
    logger = get_run_logger()

    logger.info(f"Getting Mongo connection using secret block {mongo_secret_block}")
    mongo_conn_str = Secret.load(mongo_secret_block).get()
    mongo_client = MongoClient(mongo_conn_str)
    db = mongo_client.cluebase

    exists_filter = {"domain": {"$exists": False}}
    if overwrite:
        exists_filter = {}

    clues = [clue for clue in db.clues.find(exists_filter)]
    clue_count = len(clues)
    logger.info(f"Predicting for {clue_count} clues")

    if torch.cuda.is_available():
        batch_size = 256
    else:
        batch_size = 1

    for i in range(0, clue_count, batch_size):
        batch_clues = clues[i : i + batch_size]

        logger.debug(f"Clues {i}:{i+batch_size}: {batch_clues}")
        classify_texts = [
            f"{clue['clueText']}: {clue['solution']}" for clue in batch_clues
        ]

        logger.debug(f"Classifying batch of clues {i} to {i + batch_size}")
        predicted_domains = predict(
            classify_texts, model_config, tokenizer, model, logger=logger
        )

        update_requests = [
            UpdateOne({"_id": clue["_id"]}, {"$set": {"domain": predicted_domain}})
            for clue, predicted_domain in zip(batch_clues, predicted_domains)
        ]
        logger.debug(update_requests)
        result = db.clues.bulk_write(update_requests)
        logger.debug(result)


@torch.inference_mode()
def predict(texts_to_classify, config, tokenizer, model, logger=file_logger):
    logger.debug(f"Predicting for {texts_to_classify}")
    inputs = tokenizer(
        texts_to_classify, return_tensors="pt", padding="longest", truncation=True
    )
    if torch.cuda.is_available():
        inputs = inputs.to("cuda")
    outputs = model(inputs["input_ids"], inputs["attention_mask"])

    predicted_class_idxs = torch.topk(outputs, 5, dim=1).indices

    predicted_domains = [
        [config.id2label[class_idx.item()] for class_idx in class_idxs]
        for class_idxs in predicted_class_idxs.cpu().numpy()
    ]

    final_predictions = [
        remove_unused_labels(predicted_domain_list)[0]
        for predicted_domain_list in predicted_domains
    ]

    return final_predictions


def remove_unused_labels(predicted_domains):
    return list(
        filter(
            lambda label: (label != "UNUSED"),
            predicted_domains,
        )
    )


# if __name__ == "__main__":
#     # classify_domains.serve(name="local-classify-domains")
#     classify_domains()
