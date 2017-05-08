/*******************************\
| Autor: Pawe³ Karpiñski 155085 |
| SBD Zadanie 1                 |
| 06-11-2016                    |
\*******************************/

#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define BUFFER_SIZE 100 //number of records in buffer a.k.a. blocking factor

unsigned long long readsCounter = 0;
unsigned long long writesCounter = 0;
unsigned long long phasesCounter = 0;
unsigned long long noRecords = 10;

struct Date {
	unsigned char d;
	unsigned char m;
	unsigned int y;
};

struct MyFile {
	char *name;	
	int pos;	//position of record in the begining of buffer
	int size; //number of records in file
	struct Date buffer[BUFFER_SIZE];
	int buffPos; //position of record to be read in buffer
	int noRuns; //number of runs in file
	fpos_t fpos; //position in fpos type
	int dummies;
	struct Date last;
};

int loadBuffer(struct MyFile * file) {
	FILE * fileStream;
	fileStream = fopen(file->name, "rb");
	if (fileStream) {
		readsCounter++;
		fsetpos(fileStream,&(file->fpos));
		if (file->size < file->pos + BUFFER_SIZE) {
			fread(file->buffer, sizeof(struct Date), file->size % BUFFER_SIZE, fileStream);
		}
		else {
			fread(file->buffer,sizeof(struct Date), BUFFER_SIZE, fileStream);
		}
		fgetpos(fileStream, &(file->fpos));
		fclose(fileStream);
	}
	else {
		printf("Nie mo¿na otworzyæ pliku\n");
		return -1;
	}
}

struct Date getRandDate() {
	struct tm * tm;
	time_t t;
	struct Date date;
	t = rand();
	t *= 60 * 60 * 24;
	tm = gmtime(&t);
	date.d = tm->tm_mday;
	date.m = tm->tm_mon + 1;
	date.y = tm->tm_year + 1900;
	return date;
}

void printFile(struct MyFile * file) {
	FILE * fileStream;
	struct Date * data;
	fileStream = fopen(file->name, "rb");
	if (fileStream) {
		printf("Plik %s:\n", file->name);
		data = (struct Date *)malloc(file->size * sizeof(struct Date));
		fpos_t pos = 0;
		int size = BUFFER_SIZE;
		int r = 1;
		int run = 0;
		struct Date prev;
		while (pos < file->size * sizeof(struct Date)) {
			if (pos/sizeof(struct Date) + BUFFER_SIZE >= file->size) {
				size = file->size - pos/sizeof(struct Date);
			}
			fsetpos(fileStream, &(pos));
			fread(data, sizeof(struct Date), size, fileStream);
			for (int i = 0; i < size; i++) {
				if (r > 1 && compare(&prev, &(data[i])) > 0)
					printf("-%u----------------\n", ++run);
				printf("%6u. %2u %2u %4u\n", r++, data[i].d, data[i].m, data[i].y);
				prev = data[i];
			}
			pos += sizeof(struct Date) * BUFFER_SIZE;

		}
		
		if (run)
			printf("-%u----------------\n", ++run);
		printf("\n");
		free(data);
		fclose(fileStream);
	}
}

void clearFile(struct MyFile *file) {
	FILE * fileStream;
	file->buffPos = -1;
	file->dummies = 0;
	file->fpos = 0;
	file->noRuns = 0;
	file->pos = 0;
	file->size = 0;
	fileStream = fopen(file->name, "wb");
	if (fileStream)
		fclose(fileStream);
}

char isNextRecord(struct MyFile * file) {
	FILE * fileStream;
	if (file->size == 0)
		return 0;
	if (file->pos + file->buffPos < file->size)
		return 1;
	clearFile(file);
	return 0;
}

int compare(struct Date * a, struct Date * b) {
	if (a->y > b->y)
		return 1;
	else if (a->y < b->y)
		return -1;
	else if (a->m > b->m)
		return 1;
	else if (a->m < b->m)
		return -1;
	else if (a->d > b->d)
		return 1;
	else if (a->d < b->d)
		return -1;
	else
		return 0;
}

struct Date * getRecord(struct MyFile * file) {
	if (isNextRecord(file) == 0)
		return NULL;
	if (file->buffPos >= BUFFER_SIZE) {
		file->buffPos -= BUFFER_SIZE;
		file->pos += BUFFER_SIZE;
		loadBuffer(file);
	}
	if (file->buffPos == -1) {
		file->buffPos = 0;
		loadBuffer(file);
	}
	return &(file->buffer[file->buffPos]);
}

void shiftPointer(struct MyFile * file) {
	file->buffPos++;
}

void writeRecord(struct MyFile * file, struct Date record) {
	if (file->buffPos == -1)
		file->buffPos = 0;
	file->size++;
	file->last = record;
	file->buffer[file->buffPos++] = record;
	if (file->buffPos >= BUFFER_SIZE) {
		FILE * fileStream;
		file->buffPos %= BUFFER_SIZE;
		file->pos += BUFFER_SIZE;
		if(file->pos == BUFFER_SIZE)
			fileStream = fopen(file->name, "wb");
		else
			fileStream = fopen(file->name, "ab");
		if (fileStream) {
			fsetpos(fileStream, &(file->fpos));
			fwrite(file->buffer, sizeof(struct Date), BUFFER_SIZE, fileStream);
			fgetpos(fileStream, &(file->fpos));
			fclose(fileStream);
			writesCounter++;
		}
		else {
			printf("Nie mo¿na otworzyæ pliku taœmy\n");
			return;
		}
	}
	return;
}

void myRewind(struct MyFile * file) {
	file->buffPos = -1;
	file->fpos = 0;
	file->pos = 0;
}

void flush(struct MyFile * file) {
	if (file->size == 0 || file->buffPos == -1)
		return;
	FILE * fileStream;
	fileStream = fopen(file->name, "ab");
	if (fileStream) {
		fsetpos(fileStream, &(file->fpos));
		fwrite(file->buffer, sizeof(struct Date), file->buffPos, fileStream);
		rewind(fileStream);
		fgetpos(fileStream, &(file->fpos));
		fclose(fileStream);
		writesCounter++;
		file->buffPos = 0;
	}
	else {
		printf("Nie mo¿na otworzyæ pliku taœmy %s\n", file->name);
		return;
	}
}

void distribute(struct MyFile * source, struct MyFile * t1, struct MyFile * t2) {
	char which = 1;
	int n = 1;
	struct Date tmp;
	while (isNextRecord(source)) {
		tmp = *(getRecord(source));
		shiftPointer(source);
		if (t1->size == 0 || which == 1 && !(n == 0 && (compare(&(t1->last), &tmp) > 0)) 
			|| which == 2 && n == 0 && (t2->size == 0 || compare(&(t2->last),&tmp) > 0)) {
			if (which == 2) {
				which = 1;
				n = t2->noRuns;
			}
			if (t1->size == 0 || compare(&(t1->last), &tmp) > 0) {
				n--;
				t1->noRuns++;
			}
			writeRecord(t1, tmp);
		}
		else {
			if (which == 1) {
				which = 2;
				n = t1->noRuns;
			}
			if (t2->size == 0 || compare(&(t2->last), &tmp) > 0) {
				n--;
				t2->noRuns++;
			}
			writeRecord(t2, tmp);
		}
	}
	flush(t1);
	myRewind(t1);
	flush(t2);
	myRewind(t2);
	if (which == 1)
		t1->dummies = n;
	else
		t2->dummies = n;
	return;
}

int valid_date(int dd, int mm, int yy) {
	if (mm < 1 || mm > 12) {
		return 0;
	}
	if (dd < 1) {
		return 0;
	}
	int days = 31;
	if (mm == 2) {
		days = 28;
		if (yy % 400 == 0 || (yy % 4 == 0 && yy % 100 != 0)) {
			days = 29;
		}
	}
	else if (mm == 4 || mm == 6 || mm == 9 || mm == 11) {
		days = 30;
	}
	if (dd > days) {
		return 0;
	}
	return 1;
}

void merge(struct MyFile * in1, struct MyFile * in2, struct MyFile * out) {
	out->noRuns++;
	in1->noRuns--;
	in2->noRuns--;
	struct Date last1 = { 0, 0, 0 }, last2 = { 0, 0, 0 }, curr1 = *(getRecord(in1)), curr2 = *(getRecord(in2));
	char fresh1 = 0, fresh2 = 0;

	while (isNextRecord(in1) && isNextRecord(in2) && (fresh1 == 0 || compare(&last1, &curr1) <= 0) && (fresh2 == 0 || compare(&last2, &curr2) <= 0)) {
		if (compare(&curr1, &curr2) <= 0) {
			writeRecord(out, curr1);
			shiftPointer(in1);
			fresh1 = 1;
			last1 = curr1;
			if (!isNextRecord(in1)) {
				curr1.m--;
				break;
			}
			curr1 = *(getRecord(in1));
		}
		else {
			writeRecord(out, curr2);
			shiftPointer(in2);
			fresh2 = 1;
			last2 = curr2;
			if (!isNextRecord(in2)) {
				curr2.m--;
				break;
			}
			curr2 = *(getRecord(in2));
		}
	}
	while (isNextRecord(in1) && (fresh1 == 0 || compare(&last1, &curr1) <= 0)) {
		writeRecord(out, curr1);
		shiftPointer(in1);
		fresh1 = 1;
		last1 = curr1; 
		if (!isNextRecord(in1))
			break;
		curr1 = *(getRecord(in1));
	}
	while (isNextRecord(in2) && (fresh2 == NULL || compare(&last2, &curr2) <= 0)) {
		writeRecord(out, curr2);
		shiftPointer(in2);
		fresh2 = 1;
		last2 = curr2;
		if (!isNextRecord(in2))
			break;
		curr2 = *(getRecord(in2));
	}
}

void PolyphaseSort(struct MyFile * file, char p) {
	struct MyFile * tlong, *tshort, *tout;
	struct Date record;
	tlong = calloc(1, sizeof(struct MyFile));
	tlong->name = "t1";
	tlong->buffPos = -1;
	tshort = calloc(1, sizeof(struct MyFile));
	tshort->name = "t2";
	tshort->buffPos = -1;
	tout = calloc(1, sizeof(struct MyFile));
	tout->name = "t3";
	tout->buffPos = -1;

	distribute(file, tlong, tshort);
	if (p) {
		printf("Faza %d:\n", phasesCounter);
		printFile(tlong);
		printFile(tshort);
		printFile(tout);
	}
	if (tshort->dummies + tshort->noRuns > tlong->noRuns + tlong->dummies) {
		struct MyFile *tmp;
		tmp = tshort;
		tshort = tlong;
		tlong = tmp;
	}


	while (tlong->dummies || tlong->dummies == 0 && tout->size && compare(&(tout->last), getRecord(tshort)) <= 0) {
		if (tout->size == 0 || compare(&(tout->last), getRecord(tshort)) > 0) {
			tlong->dummies--;
			tout->noRuns++;
			tshort->noRuns--;
		}
		writeRecord(tout, *(getRecord(tshort)));
		shiftPointer(tshort);
	}
	
	while (tlong->noRuns > 1) {
		int runsLeft = tlong->dummies + tlong->noRuns - tshort->noRuns;

		int k = tshort->noRuns;
		for (int i = 0; i < k; i++) {
			merge(tshort, tlong, tout);
		}		
		flush(tout);
		myRewind(tout);
		phasesCounter++;
		if (p) {
			printf("Faza %d:\n", phasesCounter);
			printFile(tlong);
			printFile(tshort);
			printFile(tout);
		}
		struct  MyFile * tmp = tlong;
		tlong = tout;
		tout = tshort;
		tshort = tmp;
	}
	//final merge
	phasesCounter++;
	merge(tshort, tlong, file);
	flush(file);
	clearFile(tlong);
	clearFile(tshort);
	clearFile(tout);
	return;
}

int main() {
	struct MyFile file;
	char c;
	file.name = "data.in";
	file.pos = 0;
	file.size = 0;
	file.buffPos = -1;

	printf("Podaj sposób wprowadzenia danych:\n1:Z klawiatury\n2:Generowane losowo\n3:Z pliku\n");
	c = getchar();
	if (c == '1' || c == '2') {
		printf("Podaj liczbê rekordów.\nn = ");
		scanf("%d", &noRecords);
	}
	switch (c) {
	case '1': {															//Dane z klawiatury
		struct Date * data;
		FILE * fileStream;
		data = (struct Date *)malloc(noRecords * sizeof(struct Date));
		printf("Podaj daty w formacie dd mm rrrr\n");
		for (int i = 0; i < noRecords; i++) {
			int dd, mm, yy;
			scanf("%u%u%u", &dd, &mm, &yy);
			if (valid_date(dd, mm, yy) == 0) {
				printf("Podano niepoprawn¹ datê\n");
				i--;
			}
			else {
				data[i].d = dd;
				data[i].m = mm;
				data[i].y = yy;
			}
		}
		fileStream = fopen(file.name, "wb");
		if (fileStream) {
			fgetpos(fileStream, &(file.fpos));
			file.size = noRecords;
			file.pos = 0;

			fwrite(data, sizeof(struct Date), noRecords, fileStream);
			free(data);
			fclose(fileStream);
		}
		else {
			printf("Nie mo¿na utworzyæ pliku\n");
			free(data);
			return 0;
		}
		break;
	}
	case '2': {								//generowanie losowo
		FILE * fileStream;
		srand(time(NULL));
		struct Date * data;
		data = (struct Date *)malloc(noRecords * sizeof(struct Date));
		for (int i = 0; i < noRecords; i++) {
			data[i] = getRandDate();
		}
		fileStream = fopen(file.name, "wb");
		if (fileStream) {
			fgetpos(fileStream, &(file.fpos));
			file.size = noRecords;
			file.pos = 0;

			fwrite(data, sizeof(struct Date), noRecords, fileStream);
			free(data);
			fclose(fileStream);
		}
		else {
			printf("Nie mo¿na utworzyæ pliku\n");
			free(data);
			return 0;
		}
		break;
	}
	case '3': { //Z pliku
		FILE * fileStream;
		printf("Podaj nazwê pliku:\n");
		file.name = (char *)calloc(20,1);
		scanf("%s", file.name);
		fileStream = fopen(file.name, "rb");
		if (fileStream) {
			fgetpos(fileStream, &(file.fpos));
			fseek(fileStream, 0, SEEK_END);
			file.size = ftell(fileStream) / sizeof(struct Date);
			rewind(fileStream);
			fclose(fileStream);
		}
		else {
			printf("Nie uda³o siê otworzyæ pliku\n");
			return 0;
		}
		break;
	}
	default:
		return 0;
	}

	printf("Czy wyœwietlaæ Tasmy po kazdej fazie(t/n)\n");
	while (c = getchar()) {
		if (c == 't' || c == 'T') {
			c = 1;
			break;
		}
		else if (c == 'n' || c == 'N') {
			c = 0;
			break;
		}
	}

	printFile(&file);

	PolyphaseSort(&file, c);

	printFile(&file);

	printf("Liczba odczytów: %d\n", readsCounter);
	printf("Liczba zapisów: %d\n", writesCounter);
	printf("Liczba faz: %d\n", phasesCounter);


	getch();
	return 0;
}