import { Pinecone, PineconeRecord } from "@pinecone-database/pinecone";
import { downloadFromS3 } from "./s3-server";
import { PDFLoader } from "langchain/document_loaders/fs/pdf";
import {Document, RecursiveCharacterTextSplitter} from "@pinecone-database/doc-splitter";
import { getEmbeddings } from "./embeddings";
import md5 from "md5";
import { Vector } from "@pinecone-database/pinecone/dist/pinecone-generated-ts-fetch";
import { convertToAscii } from "./utils";

export const getPinecone = () => {
    let pinecone: Pinecone = new Pinecone({
        apiKey: process.env.PINECONE_API_KEY!,
    });
    return pinecone;
};

type PDFPage={
    pageContent:string;
    metadata:{
        loc:{pageNumber:number}
    }
}

export async function loadS3IntoPinecone(file_key: string) {
    // obtain pdf
    console.log("downloading s3 into file system");
    const file_name = await downloadFromS3(file_key);
    if (!file_name) {
        throw new Error("could not download from s3");
    }
    console.log(file_name);
    const loader = new PDFLoader(file_name);
    const pages = (await loader.load()) as PDFPage[];

    // split and segment the pdf
    const document=await Promise.all(pages.map(prepareDocument));

    // vectorise and embed individual documents
    const vectors = await Promise.all(document.flat().map(embedDocument));

    // upload to pinecone
    const client = getPinecone();
    
    console.log('inserting vectors into pinecone');
    const namespace = convertToAscii(file_key);

    const pineconeIndex = client.index("chatpdf").namespace(namespace);

    await pineconeIndex.upsert(vectors);

    return document[0];
}

async function embedDocument(document:Document){
    try{
        const embeddings= await getEmbeddings(document.pageContent);
        const hash = md5(document.pageContent);
        return {
            id:hash,
            values:embeddings,
            metadata:{
                text:document.metadata.text,
                pageNumber:document.metadata.pageNumber
            }
        } as PineconeRecord
    }catch(error){
        console.log('error embedding document',error);
        throw error
    }
}

export const truncateStringByBytes=(str:string, numBytes:number)=>{
    const enc = new TextEncoder();
    return new TextDecoder('utf-8').decode(enc.encode(str).slice(0,numBytes));
}

export async function prepareDocument(page:PDFPage){
    let {pageContent,metadata:{loc:{pageNumber}}}=page;
    pageContent= pageContent.replace(/(\r\n|\n|\r)/gm, " ");
    const splitter = new RecursiveCharacterTextSplitter();
    const docs = await splitter.splitDocuments([
        new Document({
            pageContent,
            metadata:{
                pageNumber:pageNumber,
                text:truncateStringByBytes(pageContent,36000)
            }
        })
    ])

    return docs;
}
