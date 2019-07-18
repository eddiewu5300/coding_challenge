# Game of Thrones

## Intro

Aemon the Castle Black's maester is a close advisor of Jeor Mormont, the Lord Comman-
der. He is also the Castle Black's library director. This library has hundreds of thousands
of books and some of them are so rare you can not even nd them in the Citadel.
Searching information in this huge books collection is getting more and more challeng-
ing for Aemon as the years pass. He decided to ask Sam, his favorite trainee for some
help to nd a way to easily search information in documents from keywords. Then
Sam asked his friend Jon Snow who always liked information search and data engineering.
Jon Snow decided to use vector representation of documents. Search engines use dif-
ferent kind of document representations and one of them is the vector representation.
It gives the ability to directly use mathematical tools such as distance, similarity and
dimension reduction.
Our challenge is not about those mathematical tools but is about building an inverted
index of the documents to speed up calculations. Indeed those calculations are often
based on dot products that consume a lot of CPU and memory. Having an inverted index
simplies those dot products.
We want to write an ecient implementation to build an inverted index of a large collec-
tion of documents.
