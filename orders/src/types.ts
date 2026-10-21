export interface Item {
  sku: string;
  qty: number;
  price_cents: number;
}

export interface OrderInput {
  user_id: string;
  items: Item[];
}

export interface Order extends OrderInput {
  id: string;
  amount_cents: string;
  stage: "reserve" | "payment" | "release";
  attempts: number;
}

export interface Delivery {
  id: string;
  type: "delivery.confirmed";
  user_id: string;
  bonus_points: number;
}
