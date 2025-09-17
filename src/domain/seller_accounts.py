from src.schemas.ozon_schemas import SellerAccount


def extract_sellers(client_ids: list,
                    api_keys: list,
                    names: list) -> list[SellerAccount]:
    """
    Extracts sellers from the environment variables.
    """
    if len(client_ids) != len(api_keys) != len(names):
        raise ValueError("Client IDs, API keys, and names must have the same length.")

    return [
        SellerAccount(api_key=api_keys[i], name=names[i], client_id=client_ids[i])
        for i in range(len(client_ids))
    ]