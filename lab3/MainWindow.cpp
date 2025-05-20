#include "MainWindow.h"
#include <QLabel>
#include <QCheckBox>
#include <QPushButton>
#include <QVBoxLayout>
#include <QHBoxLayout>

MainWindow::MainWindow(QWidget *parent) : QWidget(parent) {
    QVBoxLayout *mainLayout = new QVBoxLayout(this);

    QLabel *promptLabel = new QLabel("идти сегодня в универ", this);
    mainLayout->addWidget(promptLabel);

    QCheckBox *holidayCheckBox = new QCheckBox("сегодня выходной", this);
    mainLayout->addWidget(holidayCheckBox);

    QHBoxLayout *buttonsLayout = new QHBoxLayout();

    QPushButton *yesButton = new QPushButton("да", this);
    QPushButton *noButton = new QPushButton("нет", this);
    buttonsLayout->addWidget(yesButton);
    buttonsLayout->addWidget(noButton);
    mainLayout->addLayout(buttonsLayout);

    QLabel *resultLabel = new QLabel("", this);
    mainLayout->addWidget(resultLabel);

    connect(yesButton, &QPushButton::clicked, [resultLabel]() {
        resultLabel->setText("-_-");
    });
    connect(noButton, &QPushButton::clicked, [resultLabel]() {
        resultLabel->setText(":)");
    });

    setWindowTitle("Qt Приложение");
    resize(300, 200);
}
