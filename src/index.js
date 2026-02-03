import express from "express";
import { matchRouter } from "./routes/matches.js";

const app = express();
app.use(express.json());

app.use("/matches", matchRouter);

app.listen(3000, () => {
  console.log("Server is running on port 3000");
});
