import * as fs from "fs";
import * as path from "path";
import csv from "csv-parser";
import pdf from "pdf-parse";
import axios from "axios";
import { Pinecone } from "@pinecone-database/pinecone";
import { Client } from "@elastic/elasticsearch";
import OpenAI from "openai";

const pinecone = new Pinecone({
  apiKey:
    "pcsk_4J8A6h_ejYDhucVsYskiMBzqtQG8cEw8N5grrSe4eTcq3WLdbniwj5BrgjGAm2qJGQfaB",
});
const esClient = new Client({
  node: "https://f4e65a5b128a4384919f94fe815a33f8.us-central1.gcp.cloud.es.io:443",
  auth: {
    username: "elastic",
    password: "7wlJ8i6YRomkcQSj92NEpKqn",
  },
});

// Check if the Elasticsearch client connects to the server correctly
async function checkElasticsearchConnection() {
  try {
    const isConnected = await esClient.ping();
    if (isConnected) {
      console.log("Elasticsearch connection successful");
    } else {
      console.log("Elasticsearch connection failed");
    }
  } catch (error) {
    console.error("Elasticsearch connection error:", error);
  }
}

// Call the function to check the connection
checkElasticsearchConnection();

const openai = new OpenAI({
  apiKey: "YOUR_OPENAI_API_KEY_HERE",
});

interface DataRecord {
  title: string;
  date: string;
  timestamp: number;
  subtype: string;
  description: string;
  categories: string;
  publicUrl: string;
  fileUrl: string;
}

async function indexDocument(
  docId: string,
  text: string,
  dataRecord: DataRecord
) {
  // Generate OpenAI embedding
  const embeddingResponse = await openai.embeddings.create({
    input: text,
    model: "text-embedding-ada-002",
  });
  const embedding = embeddingResponse.data[0].embedding;

  // Store in Pinecone (Vector DB)
  await pinecone.index("ulex-test").upsert([
    {
      id: docId,
      values: embedding,
      metadata: {
        text: text,
        title: dataRecord.title,
        subType: dataRecord.subtype,
        description: dataRecord.description,
        categories: dataRecord.categories,
      },
    },
  ]);

  // Store in Elasticsearch (BM25)
  await esClient.index({
    index: "documents",
    id: docId,
    body: {
      text,
      title: dataRecord.title,
      subType: dataRecord.subtype,
      description: dataRecord.description,
      categories: dataRecord.categories,
    },
  });

  console.log(`Indexed: ${docId}`);
}

const results: DataRecord[] = [];
const CHUNK_SIZE = 500;
const OVERLAP_SIZE = 250;

fs.createReadStream(path.resolve(__dirname, "data.csv"))
  .pipe(csv())
  .on("data", (data) => {
    const record: DataRecord = {
      title: data.title,
      date: data.date,
      timestamp: parseInt(data.timestamp, 10),
      subtype: data.subtype,
      description: data.description,
      categories: data.categories,
      publicUrl: data["public-url"],
      fileUrl: data["file-url"],
    };
    results.push(record);
  })
  .on("end", async () => {
    for (const record of results) {
      console.log("Processing:", record.title);
      const response = await axios.get(record.fileUrl, {
        responseType: "arraybuffer",
      });
      const pdfBuffer = Buffer.from(response.data, "binary");
      const pdfData = await pdf(pdfBuffer);
      const text = pdfData.text;

      // Split text into sentences
      const sentences = text.match(/[^.!?]+[.!?]+/g) || [];
      const chunks = [];
      let currentChunk = "";

      for (const sentence of sentences) {
        if ((currentChunk + sentence).split(/\s+/).length > CHUNK_SIZE) {
          chunks.push(currentChunk.trim());
          let words = currentChunk.split(/\s+/).slice(-OVERLAP_SIZE);
          // Remove words from the beginning until a sentence boundary is found
          while (words.length > 0 && !words[0].match(/[.!?]$/)) {
            words.shift();
          }
          words.shift();
          currentChunk = words.join(" ") + " ";
        }
        currentChunk += sentence;
      }

      // Add the last chunk if it has content
      if (currentChunk.trim().length > 0) {
        chunks.push(currentChunk.trim());
      }

      console.log(chunks.length);

      for (const [index, chunk] of chunks.entries()) {
        await indexDocument(`${record.timestamp}-${index}`, chunk, record);
      }
    }
  });
